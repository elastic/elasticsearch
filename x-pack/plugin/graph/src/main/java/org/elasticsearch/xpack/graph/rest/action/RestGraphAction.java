/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.graph.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest.TermBoost;
import org.elasticsearch.protocol.xpack.graph.Hop;
import org.elasticsearch.protocol.xpack.graph.VertexRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.graph.action.GraphExploreAction.INSTANCE;

/**
 * @see GraphExploreRequest
 */
public class RestGraphAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGraphAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
            " Specifying types in graph requests is deprecated.";

    public static final ParseField TIMEOUT_FIELD = new ParseField("timeout");
    public static final ParseField SIGNIFICANCE_FIELD = new ParseField("use_significance");
    public static final ParseField RETURN_DETAILED_INFO = new ParseField("return_detailed_stats");
    public static final ParseField SAMPLE_DIVERSITY_FIELD = new ParseField("sample_diversity");
    public static final ParseField FIELD_NAME_FIELD = new ParseField("field");
    public static final ParseField MAX_DOCS_PER_VALUE_FIELD = new ParseField("max_docs_per_value");
    public static final ParseField SAMPLE_SIZE_FIELD = new ParseField("sample_size");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD = new ParseField("shard_min_doc_count");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField VERTICES_FIELD = new ParseField("vertices");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField CONTROLS_FIELD = new ParseField("controls");
    public static final ParseField CONNECTIONS_FIELD = new ParseField("connections");
    public static final ParseField BOOST_FIELD = new ParseField("boost");
    public static final ParseField TERM_FIELD = new ParseField("term");

    public RestGraphAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, "/{index}/_graph/explore", this,
                GET, "/{index}/_xpack/graph/_explore", deprecationLogger);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, "/{index}/_graph/explore", this,
                POST, "/{index}/_xpack/graph/_explore", deprecationLogger);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                GET, "/{index}/{type}/_graph/explore", this,
                GET, "/{index}/{type}/_xpack/graph/_explore", deprecationLogger);
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
                POST, "/{index}/{type}/_graph/explore", this,
                POST, "/{index}/{type}/_xpack/graph/_explore", deprecationLogger);
    }

    @Override
    public String getName() {
        return "graph";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GraphExploreRequest graphRequest = new GraphExploreRequest(Strings.splitStringByCommaToArray(request.param("index")));
        graphRequest.indicesOptions(IndicesOptions.fromRequest(request, graphRequest.indicesOptions()));
        graphRequest.routing(request.param("routing"));
        if (request.hasParam(TIMEOUT_FIELD.getPreferredName())) {
            graphRequest.timeout(request.paramAsTime(TIMEOUT_FIELD.getPreferredName(), null));
        }
        if (false == request.hasContentOrSourceParam()) {
            throw new ElasticsearchParseException("Body missing for graph request");
        }

        Hop currentHop = graphRequest.createNextHop(null);

        try (XContentParser parser = request.contentOrSourceParamParser()) {

            XContentParser.Token token = parser.nextToken();

            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead",
                        token.name());
            }
            parseHop(parser, currentHop, graphRequest);
        }

        if (request.hasParam("type")) {
            deprecationLogger.deprecatedAndMaybeLog("graph_with_types", TYPES_DEPRECATION_MESSAGE);
            graphRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        }
        return channel -> client.execute(INSTANCE, graphRequest, new RestToXContentListener<>(channel));
    }

    private void parseHop(XContentParser parser, Hop currentHop, GraphExploreRequest graphRequest) throws IOException {
        String fieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                token = parser.nextToken();
            }

            if (token == XContentParser.Token.START_ARRAY) {
                if (VERTICES_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    parseVertices(parser, currentHop);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    currentHop.guidingQuery(parseInnerQueryBuilder(parser));
                } else if (CONNECTIONS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    parseHop(parser, graphRequest.createNextHop(null), graphRequest);
                } else if (CONTROLS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    if (currentHop.getParentHop() != null) {
                        throw new ElasticsearchParseException(
                                "Controls are a global setting that can only be set in the root " + fieldName, token.name());
                    }
                    parseControls(parser, graphRequest);
                } else {
                    throw new ElasticsearchParseException("Illegal object property in graph definition " + fieldName, token.name());

                }
            } else {
                throw new ElasticsearchParseException("Illegal property in graph definition " + fieldName, token.name());
            }
        }
    }

    private void parseVertices(XContentParser parser, Hop currentHop)
            throws IOException {
        XContentParser.Token token;

        String fieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                String field = null;
                Map<String, TermBoost> includes = null;
                HashSet<String> excludes = null;
                int size = 10;
                int minDocCount = 3;
                int shardMinDocCount = 2;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                        token = parser.nextToken();
                    }
                    if (token == XContentParser.Token.START_ARRAY) {
                        if (INCLUDE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            if (excludes != null) {
                                throw new ElasticsearchParseException(
                                        "Graph vertices definition cannot contain both "+INCLUDE_FIELD.getPreferredName()+" and "
                                        +EXCLUDE_FIELD.getPreferredName()+" clauses", token.name());
                            }
                            includes = new HashMap<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.START_OBJECT) {
                                    String includeTerm = null;
                                    float boost = 1f;
                                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                        if (token == XContentParser.Token.FIELD_NAME) {
                                            fieldName = parser.currentName();
                                        } else {
                                            if (token == XContentParser.Token.VALUE_STRING) {
                                                if (TERM_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                                                    includeTerm = parser.text();
                                                } else {
                                                    throw new ElasticsearchParseException(
                                                            "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                                            " clause has invalid property:" + fieldName);
                                                }
                                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                                if (BOOST_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                                                    boost = parser.floatValue();
                                                } else {
                                                    throw new ElasticsearchParseException(
                                                            "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                                            " clause has invalid property:" + fieldName);
                                                }
                                            } else {
                                                throw new ElasticsearchParseException(
                                                        "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                                        " clause has invalid property type:"+ token.name());

                                            }
                                        }
                                    }
                                    if (includeTerm == null) {
                                        throw new ElasticsearchParseException(
                                                "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                                " clause has missing object property for term");
                                    }
                                    includes.put(includeTerm, new TermBoost(includeTerm, boost));
                                } else if (token == XContentParser.Token.VALUE_STRING) {
                                    String term = parser.text();
                                    includes.put(term, new TermBoost(term, 1f));
                                } else {
                                    throw new ElasticsearchParseException(
                                            "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                            " clauses must be string terms or Objects with terms and boosts, not"
                                                    + token.name());
                                }
                            }
                        } else if (EXCLUDE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            if (includes != null) {
                                throw new ElasticsearchParseException(
                                        "Graph vertices definition cannot contain both "+ INCLUDE_FIELD.getPreferredName()+
                                        " and "+EXCLUDE_FIELD.getPreferredName()+" clauses", token.name());
                            }
                            excludes = new HashSet<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                excludes.add(parser.text());
                            }
                        } else {
                            throw new ElasticsearchParseException("Illegal property in graph vertices definition " + fieldName,
                                    token.name());
                        }
                    }
                    if (token == XContentParser.Token.VALUE_STRING) {
                        if (FIELD_NAME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            field = parser.text();
                        } else {
                            throw new ElasticsearchParseException("Unknown string property: [" + fieldName + "]");
                        }
                    }
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (SIZE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            size = parser.intValue();
                        } else if (MIN_DOC_COUNT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            minDocCount = parser.intValue();
                        } else if (SHARD_MIN_DOC_COUNT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            shardMinDocCount = parser.intValue();
                        } else {
                            throw new ElasticsearchParseException("Unknown numeric property: [" + fieldName + "]");
                        }
                    }
                }
                if (field == null) {
                    throw new ElasticsearchParseException("Missing field name in graph vertices definition", token.name());
                }
                VertexRequest vr = currentHop.addVertexRequest(field);
                if (includes != null) {
                    for (TermBoost tb : includes.values()) {
                        vr.addInclude(tb.getTerm(), tb.getBoost());
                    }
                }
                if (excludes != null) {
                    for (String term : excludes) {
                        vr.addExclude(term);
                    }
                }
                vr.size(size);
                vr.minDocCount(minDocCount);
                vr.shardMinDocCount(shardMinDocCount);

            }

        }

    }


    private void parseControls(XContentParser parser, GraphExploreRequest graphRequest) throws IOException {
        XContentParser.Token token;

        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SAMPLE_SIZE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    graphRequest.sampleSize(parser.intValue());
                } else if (TIMEOUT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    graphRequest.timeout(TimeValue.timeValueMillis(parser.longValue()));
                } else {
                    throw new ElasticsearchParseException("Unknown numeric property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (SIGNIFICANCE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    graphRequest.useSignificance(parser.booleanValue());
                } else if (RETURN_DETAILED_INFO.match(fieldName, parser.getDeprecationHandler())) {
                    graphRequest.returnDetailedInfo(parser.booleanValue());
                } else{
                    throw new ElasticsearchParseException("Unknown boolean property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (TIMEOUT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    graphRequest.timeout(TimeValue.parseTimeValue(parser.text(), null, "timeout"));
                } else {
                    throw new ElasticsearchParseException("Unknown numeric property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SAMPLE_DIVERSITY_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                            token = parser.nextToken();
                        }
                        if (FIELD_NAME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            graphRequest.sampleDiversityField(parser.text());
                        } else if (MAX_DOCS_PER_VALUE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                            graphRequest.maxDocsPerDiversityValue(parser.intValue());
                        } else {
                            throw new ElasticsearchParseException("Unknown property: [" + fieldName + "]");
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("Unknown object property: [" + fieldName + "]");
                }
            } else {
                throw new ElasticsearchParseException("Unknown object property: [" + fieldName + "]");
            }
        }
    }

}
