package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kyra.wkh
 **/
public class CoordinatingIndiceStats implements Writeable, ToXContentFragment {

    public String indexName;
    public final Map<String, CounterMetric> counterMetricMap;
    public final Map<String, MeanMetric> meanMatricMap;

    public CoordinatingIndiceStats(String indexName, Map<String, CounterMetric> counterMetricMap, Map<String, MeanMetric> meanMetricMap) {
        this.indexName = indexName;
        this.counterMetricMap = (counterMetricMap == null ? new ConcurrentHashMap<>() : counterMetricMap);
        this.meanMatricMap = (meanMetricMap == null ? new ConcurrentHashMap<>() : meanMetricMap);
    }

    public CoordinatingIndiceStats(StreamInput in) throws IOException {
        this.counterMetricMap = new ConcurrentHashMap<>();
        this.meanMatricMap = new ConcurrentHashMap<>();

        this.indexName = in.readString();
        counterMetricMap.putAll(in.readMap(StreamInput::readString, stream -> {
            long value = stream.readVLong();
            CounterMetric counterMetric = new CounterMetric();
            counterMetric.inc(value);
            return counterMetric;
        }));

        meanMatricMap.putAll(in.readMap(StreamInput::readString, stream ->
            new MeanMetric(stream.readVLong(), stream.readVLong())
        ));
    }

    public CoordinatingIndiceStats add(CoordinatingIndiceStats other) {
        for(String metricName : other.meanMatricMap.keySet()) {
            MeanMetric current = meanMatricMap.get(metricName);
            MeanMetric otherValue = other.meanMatricMap.get(metricName);
            if (current == null) {
                meanMatricMap.put(metricName, new MeanMetric(otherValue.count(), otherValue.sum()));
            } else {
                current.merge(otherValue);
            }
        }

        for(String metricName : other.counterMetricMap.keySet()) {
            CounterMetric current = counterMetricMap.get(metricName);
            CounterMetric otherValue = other.counterMetricMap.get(metricName);
            if (current == null) {
                CounterMetric counterMetric = new CounterMetric();
                counterMetric.inc(otherValue.count());
                counterMetricMap.put(metricName, counterMetric);
            } else {
                current.inc(otherValue.count());
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(counterMetricMap, StreamOutput::writeString, (stream, counterMetric) -> stream.writeVLong(counterMetric.count()));
        out.writeMap(meanMatricMap, StreamOutput::writeString, (stream, meanMetric) -> {
            stream.writeVLong(meanMetric.count());
            stream.writeVLong(meanMetric.sum());
        });
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", indexName);
        builder.startObject("stats");
        for(Entry<String, CounterMetric> counterMetricEntry : counterMetricMap.entrySet()) {
            String metricName = counterMetricEntry.getKey();
            CounterMetric metric = counterMetricEntry.getValue();
            builder.field(metricName, metric.count());
        }
        for(Entry<String, MeanMetric> counterMetricEntry : meanMatricMap.entrySet()) {
            String metricName = counterMetricEntry.getKey();
            MeanMetric meanMetric = counterMetricEntry.getValue();
            builder.field(metricName, meanMetric.sum());
        }
        builder.endObject();
        return builder;
    }
}
