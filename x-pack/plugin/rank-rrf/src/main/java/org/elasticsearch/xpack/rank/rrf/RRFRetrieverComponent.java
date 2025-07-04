package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RRFRetrieverComponent implements ToXContentObject {
    
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    static final float DEFAULT_WEIGHT = 1f;

    final RetrieverBuilder retriever;
    final float weight;
    
    public RRFRetrieverComponent(RetrieverBuilder retrieverBuilder, Float weight) {
        assert retrieverBuilder != null;
        this.retriever = retrieverBuilder;
        this.weight = weight == null ? DEFAULT_WEIGHT : weight;
        if (this.weight < 0) {
            throw new IllegalArgumentException("[weight] must be non-negative");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), retriever);
        builder.field(WEIGHT_FIELD.getPreferredName(), weight);
        return builder;
    }


    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<RRFRetrieverComponent, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "rrf_component",
        false,
        args -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            Float weight = (Float) args[1];
            return new RRFRetrieverComponent(retrieverBuilder, weight);
        }
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder innerRetriever = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(innerRetriever.getName());
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), WEIGHT_FIELD);
    }

    public static RRFRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }
}
