/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.graph.action.GraphExploreRequest;
import org.elasticsearch.xpack.graph.action.GraphExploreRequest.TermBoost;
import org.elasticsearch.xpack.graph.action.Hop;
import org.elasticsearch.xpack.graph.action.VertexRequest;
import org.elasticsearch.xpack.rest.XPackRestHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.graph.action.GraphExploreAction.INSTANCE;

/**
 * @see GraphExploreRequest
 */
public class RestGraphAction extends XPackRestHandler {
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

    public RestGraphAction(Settings settings, RestController controller) {
        super(settings);

        // @deprecated Remove in 6.0
        // NOTE: Old versions did not end with "/_explore"; they were just "/explore"
        controller.registerWithDeprecatedHandler(GET, "/{index}" + URI_BASE + "/_graph/_explore", this,
                                                 GET, "/{index}/_graph/explore", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, "/{index}" + URI_BASE + "/_graph/_explore", this,
                                                 POST, "/{index}/_graph/explore", deprecationLogger);
        controller.registerWithDeprecatedHandler(GET, "/{index}/{type}" + URI_BASE + "/_graph/_explore", this,
                                                 GET, "/{index}/{type}/_graph/explore", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, "/{index}/{type}" + URI_BASE + "/_graph/_explore", this,
                                                 POST, "/{index}/{type}/_graph/explore", deprecationLogger);
    }

    @Override
    public RestChannelConsumer doPrepareRequest(final RestRequest request, final XPackClient client) throws IOException {
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
            QueryParseContext context = new QueryParseContext(parser);

            XContentParser.Token token = parser.nextToken();

            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead",
                        token.name());
            }
            parseHop(parser, context, currentHop, graphRequest);
        }

        graphRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        return channel -> client.es().execute(INSTANCE, graphRequest, new RestToXContentListener<>(channel));
    }

    private void parseHop(XContentParser parser, QueryParseContext context, Hop currentHop,
            GraphExploreRequest graphRequest) throws IOException {
        String fieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                token = parser.nextToken();
            }

            if (token == XContentParser.Token.START_ARRAY) {
                if (VERTICES_FIELD.match(fieldName)) {
                    parseVertices(parser, currentHop);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(fieldName)) {
                    currentHop.guidingQuery(context.parseInnerQueryBuilder());
                } else if (CONNECTIONS_FIELD.match(fieldName)) {
                    parseHop(parser, context, graphRequest.createNextHop(null), graphRequest);
                } else if (CONTROLS_FIELD.match(fieldName)) {
                    if (currentHop.getParentHop() != null) {
                        throw new ElasticsearchParseException(
                                "Controls are a global setting that can only be set in the root " + fieldName, token.name());
                    }
                    parseControls(parser, context, graphRequest);
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
                        if (INCLUDE_FIELD.match(fieldName)) {
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
                                                if (TERM_FIELD.match(fieldName)) {
                                                    includeTerm = parser.text();
                                                } else {
                                                    throw new ElasticsearchParseException(
                                                            "Graph vertices definition " + INCLUDE_FIELD.getPreferredName() +
                                                            " clause has invalid property:" + fieldName);
                                                }
                                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                                if (BOOST_FIELD.match(fieldName)) {
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
                        } else if (EXCLUDE_FIELD.match(fieldName)) {
                            if (includes != null) {
                                throw new ElasticsearchParseException(
                                        "Graph vertices definition cannot contain both "+ INCLUDE_FIELD.getPreferredName()+
                                        " and "+EXCLUDE_FIELD.getPreferredName()+" clauses", token.name());
                            }
                            excludes = new HashSet<String>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                excludes.add(parser.text());
                            }
                        } else {
                            throw new ElasticsearchParseException("Illegal property in graph vertices definition " + fieldName,
                                    token.name());
                        }
                    }
                    if (token == XContentParser.Token.VALUE_STRING) {
                        if (FIELD_NAME_FIELD.match(fieldName)) {
                            field = parser.text();
                        } else {
                            throw new ElasticsearchParseException("Unknown string property: [" + fieldName + "]");
                        }
                    }
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (SIZE_FIELD.match(fieldName)) {
                            size = parser.intValue();
                        } else if (MIN_DOC_COUNT_FIELD.match(fieldName)) {
                            minDocCount = parser.intValue();
                        } else if (SHARD_MIN_DOC_COUNT_FIELD.match(fieldName)) {
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


    private void parseControls(XContentParser parser, QueryParseContext context, GraphExploreRequest graphRequest) throws IOException {
        XContentParser.Token token;

        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SAMPLE_SIZE_FIELD.match(fieldName)) {
                    graphRequest.sampleSize(parser.intValue());
                } else if (TIMEOUT_FIELD.match(fieldName)) {
                    graphRequest.timeout(TimeValue.timeValueMillis(parser.longValue()));
                } else {
                    throw new ElasticsearchParseException("Unknown numeric property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (SIGNIFICANCE_FIELD.match(fieldName)) {
                    graphRequest.useSignificance(parser.booleanValue());
                } else if (RETURN_DETAILED_INFO.match(fieldName)) {
                    graphRequest.returnDetailedInfo(parser.booleanValue());
                } else{
                    throw new ElasticsearchParseException("Unknown boolean property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (TIMEOUT_FIELD.match(fieldName)) {
                    graphRequest.timeout(TimeValue.parseTimeValue(parser.text(), null, "timeout"));
                } else {
                    throw new ElasticsearchParseException("Unknown numeric property: [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SAMPLE_DIVERSITY_FIELD.match(fieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            fieldName = parser.currentName();
                            token = parser.nextToken();
                        }
                        if (FIELD_NAME_FIELD.match(fieldName)) {
                            graphRequest.sampleDiversityField(parser.text());
                        } else if (MAX_DOCS_PER_VALUE_FIELD.match(fieldName)) {
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
