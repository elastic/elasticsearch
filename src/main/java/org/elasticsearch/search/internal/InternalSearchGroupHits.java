package org.elasticsearch.search.internal;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;

import static org.elasticsearch.search.SearchShardTarget.readSearchShardTarget;
import static org.elasticsearch.search.internal.InternalSearchHit.readSearchHit;

/**
 * @author Artem Redkin <artem@redkin.me>
 */
public class InternalSearchGroupHits extends InternalSearchHits {

    private static final ThreadLocal<ThreadLocals.CleanableValue<StreamContext>> cache = new ThreadLocal<ThreadLocals.CleanableValue<StreamContext>>() {
        @Override
        protected ThreadLocals.CleanableValue<StreamContext> initialValue() {
            return new ThreadLocals.CleanableValue<StreamContext>(new StreamContext());
        }
    };

    public static final InternalSearchHit[] EMPTY = new InternalSearchHit[0];

    private InternalSearchHit[] hits;
    private Map<String, List<InternalSearchHit>> group;

    public long totalHits;

    private float maxScore;
    
    InternalSearchGroupHits() {
    }

    public InternalSearchGroupHits(Map<String, List<InternalSearchHit>> group, InternalSearchHit[] hits, long totalHits, float maxScore) {
        super(hits, totalHits, maxScore);
        this.group = group;
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
    }

    public void shardTarget(SearchShardTarget shardTarget) {
        for (InternalSearchHit hit : hits) {
            hit.shardTarget(shardTarget);
        }
    }

    public long totalHits() {
        return totalHits;
    }

    @Override
    public long getTotalHits() {
        return totalHits();
    }

    @Override
    public float maxScore() {
        return this.maxScore;
    }

    @Override
    public float getMaxScore() {
        return maxScore();
    }

    public SearchHit[] hits() {
        return this.hits;
    }

    @Override
    public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override
    public SearchHit[] getHits() {
        return hits();
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Iterators.forArray(hits());
    }

    public InternalSearchHit[] internalHits() {
        return this.hits;
    }

    static final class Fields {
        static final XContentBuilderString HITS = new XContentBuilderString("hits");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MAX_SCORE = new XContentBuilderString("max_score");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (String term : group.keySet()) {
            builder.startObject(term);
            builder.startArray(Fields.HITS);
            for (InternalSearchHit hit : group.get(term)) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
        }
        return builder;
    }

    public static InternalSearchGroupHits readSearchHits(StreamInput in, StreamContext context) throws IOException {
        InternalSearchGroupHits hits = new InternalSearchGroupHits();
        hits.readFrom(in, context);
        return hits;
    }

    public static InternalSearchGroupHits readSearchHits(StreamInput in) throws IOException {
        InternalSearchGroupHits hits = new InternalSearchGroupHits();
        hits.readFrom(in);
        return hits;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        readFrom(in, streamContext().streamShardTarget(StreamContext.ShardTargetType.LOOKUP));
    }

    public void readFrom(StreamInput in, StreamContext context) throws IOException {
        totalHits = in.readVLong();
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            if (context.streamShardTarget() == StreamContext.ShardTargetType.LOOKUP) {
                // read the lookup table first
                int lookupSize = in.readVInt();
                for (int i = 0; i < lookupSize; i++) {
                    context.handleShardLookup().put(in.readVInt(), readSearchShardTarget(in));
                }
            }

            hits = new InternalSearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = readSearchHit(in, context);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, streamContext().streamShardTarget(StreamContext.ShardTargetType.LOOKUP));
    }

    public void writeTo(StreamOutput out, StreamContext context) throws IOException {
        out.writeVLong(totalHits);
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            if (context.streamShardTarget() == StreamContext.ShardTargetType.LOOKUP) {
                // start from 1, 0 is for null!
                int counter = 1;
                for (InternalSearchHit hit : hits) {
                    if (hit.shard() != null) {
                        Integer handle = context.shardHandleLookup().get(hit.shard());
                        if (handle == null) {
                            context.shardHandleLookup().put(hit.shard(), counter++);
                        }
                    }
                }
                out.writeVInt(context.shardHandleLookup().size());
                if (!context.shardHandleLookup().isEmpty()) {
                    for (Map.Entry<SearchShardTarget, Integer> entry : context.shardHandleLookup().entrySet()) {
                        out.writeVInt(entry.getValue());
                        entry.getKey().writeTo(out);
                    }
                }
            }

            for (InternalSearchHit hit : hits) {
                hit.writeTo(out, context);
            }
        }
    }

    
}
