/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.sort.SortParseElement;

import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;

public class PercolateDocumentParser {

    private final HighlightPhase highlightPhase;
    private final SortParseElement sortParseElement;
    private final AggregationPhase aggregationPhase;

    @Inject
    public PercolateDocumentParser(HighlightPhase highlightPhase, SortParseElement sortParseElement,
                                   AggregationPhase aggregationPhase) {
        this.highlightPhase = highlightPhase;
        this.sortParseElement = sortParseElement;
        this.aggregationPhase = aggregationPhase;
    }

    public ParsedDocument parse(PercolateShardRequest request, PercolateContext context, MapperService mapperService, QueryShardContext queryShardContext) {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            if (request.docSource() != null && request.docSource().length() != 0) {
                return parseFetchedDoc(context, request.docSource(), mapperService, request.shardId().getIndex(), request.documentType());
            } else {
                return null;
            }
        }

        // TODO: combine all feature parse elements into one map
        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
        Map<String, ? extends SearchParseElement> aggregationElements = aggregationPhase.parseElements();

        ParsedDocument doc = null;
        // Some queries (function_score query when for decay functions) rely on a SearchContext being set:
        // We switch types because this context needs to be in the context of the percolate queries in the shard and
        // not the in memory percolate doc
        String[] previousTypes = context.types();
        context.types(new String[]{PercolatorService.TYPE_NAME});
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source);) {
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        if (doc != null) {
                            throw new ElasticsearchParseException("Either specify doc or get, not both");
                        }

                        DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        String index = context.shardTarget().index();
                        doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(request.documentType()).flyweight(true));
                        if (docMapper.getMapping() != null) {
                            doc.addDynamicMappingsUpdate(docMapper.getMapping());
                        }
                        // the document parsing exists the "doc" object, so we need to set the new current field.
                        currentFieldName = parser.currentName();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    SearchParseElement element = hlElements.get(currentFieldName);
                    if (element == null) {
                        element = aggregationElements.get(currentFieldName);
                    }

                    if ("query".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        context.percolateQuery(queryShardContext.parse(parser).query());
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        Query filter = queryShardContext.parseInnerFilter(parser).query();
                        context.percolateQuery(new ConstantScoreQuery(filter));
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if (element != null) {
                        element.parse(parser, context);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    }
                } else if (token == null) {
                    break;
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.size(parser.intValue());
                        if (context.size() < 0) {
                            throw new ElasticsearchParseException("size is set to [{}] and is expected to be higher or equal to 0", context.size());
                        }
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if ("track_scores".equals(currentFieldName) || "trackScores".equals(currentFieldName)) {
                        context.trackScores(parser.booleanValue());
                    }
                }
            }

            // We need to get the actual source from the request body for highlighting, so parse the request body again
            // and only get the doc source.
            if (context.highlight() != null) {
                parser.close();
                currentFieldName = null;
                try (XContentParser parserForHighlighter = XContentFactory.xContent(source).createParser(source)) {
                    token = parserForHighlighter.nextToken();
                    assert token == XContentParser.Token.START_OBJECT;
                    while ((token = parserForHighlighter.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parserForHighlighter.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("doc".equals(currentFieldName)) {
                                BytesStreamOutput bStream = new BytesStreamOutput();
                                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, bStream);
                                builder.copyCurrentStructure(parserForHighlighter);
                                builder.close();
                                doc.setSource(bStream.bytes());
                                break;
                            } else {
                                parserForHighlighter.skipChildren();
                            }
                        } else if (token == null) {
                            break;
                        }
                    }
                }
            }

        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            context.types(previousTypes);
        }

        if (request.docSource() != null && request.docSource().length() != 0) {
            if (doc != null) {
                throw new IllegalArgumentException("Can't specify the document to percolate in the source of the request and as document id");
            }

            doc = parseFetchedDoc(context, request.docSource(), mapperService, request.shardId().getIndex(), request.documentType());
        }

        if (doc == null) {
            throw new IllegalArgumentException("Nothing to percolate");
        }

        return doc;
    }

    private void parseSort(XContentParser parser, PercolateContext context) throws Exception {
        context.trackScores(true);
        sortParseElement.parse(parser, context);
        // null, means default sorting by relevancy
        if (context.sort() != null) {
            throw new ElasticsearchParseException("Only _score desc is supported");
        }
    }

    private ParsedDocument parseFetchedDoc(PercolateContext context, BytesReference fetchedDoc, MapperService mapperService, String index, String type) {
        try (XContentParser parser = XContentFactory.xContent(fetchedDoc).createParser(fetchedDoc)) {
            DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(type);
            ParsedDocument doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(type).flyweight(true));
            if (doc == null) {
                throw new ElasticsearchParseException("No doc to percolate in the request");
            }
            if (context.highlight() != null) {
                doc.setSource(fetchedDoc);
            }
            return doc;
        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        }
    }

}
