package org.elasticsearch.benchmark.search.fetch.subphase;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.filtering.FilterPath;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.lookup.SourceLookup;
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
    private FilterPath[] includesFilters;
    private FilterPath[] excludesFilters;

    @Param({ "tiny", "short", "one_4k_field", "one_4m_field" })
    private String source;
    @Param({ "message" })
    private String includes;
    @Param({ "" })
    private String excludes;

    @Setup
    public void setup() throws IOException {
        switch (source) {
            case "tiny":
                sourceBytes = new BytesArray("{\"message\": \"short\"}");
                break;
            case "short":
                sourceBytes = read300BytesExample();
                break;
            case "one_4k_field":
                sourceBytes = buildBigExample("huge".repeat(1024));
                break;
            case "one_4m_field":
                sourceBytes = buildBigExample("huge".repeat(1024 * 1024));
                break;
            default:
                throw new IllegalArgumentException("Unknown source [" + source + "]");
        }
        fetchContext = new FetchSourceContext(
            true,
            Strings.splitStringByCommaToArray(includes),
            Strings.splitStringByCommaToArray(excludes)
        );
        includesSet = Set.of(fetchContext.includes());
        excludesSet = Set.of(fetchContext.excludes());
        includesFilters = FilterPath.compile(Set.of(fetchContext.includes()));
        excludesFilters = FilterPath.compile(Set.of(fetchContext.excludes()));
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
    public BytesReference filterObjects() throws IOException {
        SourceLookup lookup = new SourceLookup();
        lookup.setSource(sourceBytes);
        Object value = lookup.filter(fetchContext);
        return FetchSourcePhase.objectToBytes(value, XContentType.JSON, Math.min(1024, lookup.internalSourceRef().length()));
    }

    @Benchmark
    public BytesReference filterXContentOnParser() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, sourceBytes.length()));
        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), streamOutput);
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    sourceBytes.streamInput(),
                    includesFilters,
                    excludesFilters
                )
        ) {
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
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, sourceBytes.streamInput())
        ) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }
}
