package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.metrics.MetricsConstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author kyra.wkh
 */
public class CoordinatingStats implements Writeable, ToXContentFragment {

    private final List<CoordinatingIndiceStats> indiceStatsList;

    public CoordinatingStats(List<CoordinatingIndiceStats> indiceStatsList) {
        this.indiceStatsList = indiceStatsList;
    }

    public CoordinatingStats(StreamInput in) throws IOException {
        int size = in.readVInt();
        this.indiceStatsList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            indiceStatsList.add(new CoordinatingIndiceStats(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(indiceStatsList.size());
        for (CoordinatingIndiceStats stats : indiceStatsList) {
            stats.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MetricsConstant.COORDINATING);
        builder.startObject("total");
        CoordinatingIndiceStats totalStats = getTotal();
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startArray("indices");
        for (CoordinatingIndiceStats stats : getIndiceStats()) {
            builder.startObject();
            stats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    protected CoordinatingIndiceStats getTotal() {
        CoordinatingIndiceStats totalIndiceStats = new CoordinatingIndiceStats("_all", new HashMap<>(), new HashMap<>());
        for (CoordinatingIndiceStats stats : indiceStatsList) {
            totalIndiceStats.add(stats);
        }
        return totalIndiceStats;
    }

    protected List<CoordinatingIndiceStats> getIndiceStats() {
        return indiceStatsList;
    }
}
