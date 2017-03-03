package org.apache.lucene.search;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
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
 * ({ClassicSimilarity}), you must override {ClassicSimilarity#scorePayload(int, int, int, BytesRef)},
 * which returns 1 by default.
 *
 * org.apache.lucene.search.similarities.Similarity.SimScorer#computePayloadFactor(int, int, int, BytesRef)
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
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    SpanWeight innerWeight = wrappedQuery.createWeight(searcher, scoreMode, boost);
    if (!scoreMode.needsScores())
      return innerWeight;
    return new PayloadSpanWeight(searcher, innerWeight, boost);
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

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return innerWeight.isCacheable(ctx);
    }

    public PayloadSpanWeight(IndexSearcher searcher, SpanWeight innerWeight, float boost) throws IOException {
      super(PayloadScoreQuery.this, searcher, null, boost);
      this.innerWeight = innerWeight;
      this.boost = 1.0f;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      innerWeight.extractTermStates(contexts);
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
      LeafSimScorer docScorer = innerWeight.getSimScorer(context);
      PayloadSpans payloadSpans = new PayloadSpans(spans, docScorer.getSimScorer());
      return new PayloadSpanScorer(this, payloadSpans, docScorer, boost);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      innerWeight.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      PayloadSpanScorer scorer = scorer(context);
      if (scorer == null || scorer.iterator().advance(doc) != doc)
        return Explanation.noMatch("No match");

      scorer.score();  // force freq calculation
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

    private int decodeInt(byte [] bytes, int offset){
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
            | ((bytes[offset + 2] & 0xFF) <<  8) |  (bytes[offset + 3] & 0xFF);
    }

    private float decodeFloat(byte [] bytes, int offset){
        return Float.intBitsToFloat(decodeInt(bytes, offset));
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      BytesRef payload = postings.getPayload();
      if (payload == null)
        return;

      float freq = decodeFloat(payload.bytes, payload.offset);
      float payloadFactor = docScorer.score(freq, 1);
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

    private PayloadSpanScorer(SpanWeight weight, PayloadSpans spans, LeafSimScorer docScorer, float boost) throws IOException {
      super(weight, spans, docScorer);
      this.spans = spans;
    }

    protected float getPayloadScore() {
      return spans.payloadsSeen > 0 ? spans.payloadScore : 0;
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