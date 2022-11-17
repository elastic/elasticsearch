package org.elasticsearch.benchmark.search.fetch.subphase;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FetchSourcePhaseBenchmark {
    private BytesReference sourceBytes;
    private FetchSourceContext fetchContext;
    private Set<String> includesSet;
    private Set<String> excludesSet;
    private XContentParserConfiguration parserConfig;

    @Param({ "tiny", "short", "one_4k_field", "one_4m_field" })
    private String source;
    @Param({ "message" })
    private String includes;
    @Param({ "" })
    private String excludes;

    @Setup
    public void setup() throws IOException {
        sourceBytes = switch (source) {
            case "tiny" -> new BytesArray("{\"message\": \"short\"}");
            case "short" -> read300BytesExample();
            case "one_4k_field" -> buildBigExample("huge".repeat(1024));
            case "one_4m_field" -> buildBigExample("huge".repeat(1024 * 1024));
            default -> throw new IllegalArgumentException("Unknown source [" + source + "]");
        };
        fetchContext = FetchSourceContext.of(
            true,
            Strings.splitStringByCommaToArray(includes),
            Strings.splitStringByCommaToArray(excludes)
        );
        includesSet = Set.of(fetchContext.includes());
        excludesSet = Set.of(fetchContext.excludes());
        parserConfig = XContentParserConfiguration.EMPTY.withFiltering(includesSet, excludesSet, false);
    }

    private BytesReference read300BytesExample() throws IOException {
        return Streams.readFully(FetchSourcePhaseBenchmark.class.getResourceAsStream("300b_example.json"));
    }

    private BytesReference buildBigExample(String extraText) throws IOException {
        String bigger = read300BytesExample().utf8ToString();
        bigger = "{\"huge\": \"" + extraText + "\"," + bigger.substring(1);
        return new BytesArray(bigger);
    }

    @Benchmark
    public BytesReference filterSourceMap() {
        Source bytesSource = Source.fromBytes(sourceBytes);
        return fetchContext.filter().filterMap(bytesSource).internalSourceRef();
    }

    @Benchmark
    public BytesReference filterSourceBytes() {
        Source bytesSource = Source.fromBytes(sourceBytes);
        return fetchContext.filter().filterBytes(bytesSource).internalSourceRef();
    }

    @Benchmark
    public BytesReference filterXContentOnParser() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, sourceBytes.length()));
        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), streamOutput);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(parserConfig, sourceBytes.streamInput())) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    @Benchmark
    public BytesReference filterXContentOnBuilder() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, sourceBytes.length()));
        XContentBuilder builder = new XContentBuilder(
            XContentType.JSON.xContent(),
            streamOutput,
            includesSet,
            excludesSet,
            XContentType.JSON.toParsedMediaType()
        );
        try (
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, sourceBytes.streamInput())
        ) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }
}
