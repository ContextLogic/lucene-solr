package org.apache.lucene.search;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.search.spans.FilterSpans;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * A Query class that uses a max payload score as the score of a wrapped SpanQuery.
 * Directly copied and modified from PayloadScoreQuery.
 *
 * NOTE: In order to take advantage of this with the default scoring implementation
 * ({@link ClassicSimilarity}), you must override {@link ClassicSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 *
 * @see org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
 */
public class PayloadScoreQuery extends SpanQuery {

  private final SpanQuery wrappedQuery;
  private final boolean includeSpanScore;

  /**
   * Creates a new PayloadScoreQuery
   * @param wrappedQuery the query to wrap
   * @param includeSpanScore include both span score and payload score in the scoring algorithm
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery, boolean includeSpanScore) {
    this.wrappedQuery = Objects.requireNonNull(wrappedQuery);
    this.includeSpanScore = includeSpanScore;
  }

  /**
   * Creates a new PayloadScoreQuery that includes the underlying span scores
   * @param wrappedQuery the query to wrap
   */
  public PayloadScoreQuery(SpanQuery wrappedQuery) {
    this(wrappedQuery, true);
  }

  @Override
  public String getField() {
    return wrappedQuery.getField();
  }

  @Override
  public String toString(String field) {
    return "PayloadSpanQuery[" + wrappedQuery.toString(field) + "]";
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    SpanWeight innerWeight = wrappedQuery.createWeight(searcher, needsScores);
    if (!needsScores)
      return innerWeight;
    return new PayloadSpanWeight(searcher, innerWeight);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(PayloadScoreQuery other) {
    return wrappedQuery.equals(other.wrappedQuery);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + Objects.hashCode(wrappedQuery);
    return result;
  }

  private class PayloadSpanWeight extends SpanWeight {

    private final SpanWeight innerWeight;
    private float boost;

    public PayloadSpanWeight(IndexSearcher searcher, SpanWeight innerWeight) throws IOException {
      super(PayloadScoreQuery.this, searcher, null);
      this.innerWeight = innerWeight;
      this.boost = 1.0f;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      innerWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
      return innerWeight.getSpans(ctx, requiredPostings.atLeast(Postings.PAYLOADS));
    }

    @Override
    public PayloadSpanScorer scorer(LeafReaderContext context) throws IOException {
      Spans spans = getSpans(context, Postings.PAYLOADS);
      if (spans == null)
        return null;
      SimScorer docScorer = innerWeight.getSimScorer(context);
      PayloadSpans payloadSpans = new PayloadSpans(spans, docScorer);
      return new PayloadSpanScorer(this, payloadSpans, docScorer, boost);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      innerWeight.extractTerms(terms);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return innerWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      innerWeight.normalize(queryNorm, topLevelBoost);
      this.boost = topLevelBoost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadSpanScorer scorer = scorer(context);
      if (scorer == null || scorer.iterator().advance(doc) != doc)
        return Explanation.noMatch("No match");

      scorer.freq();  // force freq calculation
      Explanation payloadExpl = scorer.getPayloadExplanation();

      if (includeSpanScore) {
        SpanWeight innerWeight = ((PayloadSpanWeight) scorer.getWeight()).innerWeight;
        Explanation innerExpl = innerWeight.explain(context, doc);
        return Explanation.match(scorer.scoreCurrentDoc(), "PayloadSpanQuery, product of:", innerExpl, payloadExpl);
      }

      return scorer.getPayloadExplanation();
    }
  }

  private class PayloadSpans extends FilterSpans implements SpanCollector {

    private final SimScorer docScorer;
    public int payloadsSeen;
    public float payloadScore;

    private PayloadSpans(Spans in, SimScorer docScorer) {
      super(in);
      this.docScorer = docScorer;
    }

    @Override
    protected AcceptStatus accept(Spans candidate) throws IOException {
      return AcceptStatus.YES;
    }

    @Override
    protected void doStartCurrentDoc() {
      payloadScore = 0;
      payloadsSeen = 0;
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      BytesRef payload = postings.getPayload();
      if (payload == null)
        return;
      float payloadFactor = docScorer.computePayloadFactor(docID(), in.startPosition(), in.endPosition(), payload);
      payloadScore = Math.max(payloadScore, payloadFactor);
      payloadsSeen++;
    }

    @Override
    public void reset() {}

    @Override
    protected void doCurrentSpans() throws IOException {
      in.collect(this);
    }
  }

  private class PayloadSpanScorer extends SpanScorer {

    private final PayloadSpans spans;
    private final float boost;

    private PayloadSpanScorer(SpanWeight weight, PayloadSpans spans, Similarity.SimScorer docScorer, float boost) throws IOException {
      super(weight, spans, docScorer);
      this.spans = spans;
      this.boost = boost;
    }

    protected float getPayloadScore() {
      // Hack! Always apply boost to payload score.
      return spans.payloadsSeen > 0 ? spans.payloadScore * boost : boost;
    }

    protected Explanation getPayloadExplanation() {
      return Explanation.match(
          getPayloadScore(),
          getClass().getSimpleName() + ".docScore()");
    }

    protected float getSpanScore() throws IOException {
      return super.scoreCurrentDoc();
    }

    @Override
    protected float scoreCurrentDoc() throws IOException {
      if (includeSpanScore)
        return getSpanScore() * getPayloadScore();
      return getPayloadScore();
    }

  }
}

