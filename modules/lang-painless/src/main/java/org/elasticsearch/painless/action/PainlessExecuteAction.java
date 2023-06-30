/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.painless.action;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class PainlessExecuteAction extends ActionType<PainlessExecuteAction.Response> {

    public static final PainlessExecuteAction INSTANCE = new PainlessExecuteAction();
    private static final String NAME = "cluster:admin/scripts/painless/execute";

    private PainlessExecuteAction() {
        super(NAME, Response::new);
    }

    public static class Request extends SingleShardRequest<Request> implements ToXContentObject {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ParseField CONTEXT_SETUP_FIELD = new ParseField("context_setup");
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request",
            args -> new Request((Script) args[0], (String) args[1], (ContextSetup) args[2])
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CONTEXT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ContextSetup::parse, CONTEXT_SETUP_FIELD);
        }

        private static Map<String, ScriptContext<?>> getSupportedContexts() {
            Map<String, ScriptContext<?>> contexts = new HashMap<>();
            contexts.put(PainlessTestScript.CONTEXT.name, PainlessTestScript.CONTEXT);
            contexts.put(FilterScript.CONTEXT.name, FilterScript.CONTEXT);
            contexts.put(ScoreScript.CONTEXT.name, ScoreScript.CONTEXT);
            for (ScriptContext<?> runtimeFieldsContext : ScriptModule.RUNTIME_FIELDS_CONTEXTS) {
                contexts.put(runtimeFieldsContext.name, runtimeFieldsContext);
            }
            return Collections.unmodifiableMap(contexts);
        }

        static final Map<String, ScriptContext<?>> SUPPORTED_CONTEXTS = getSupportedContexts();

        static ScriptContext<?> fromScriptContextName(String name) {
            ScriptContext<?> scriptContext = SUPPORTED_CONTEXTS.get(name);
            if (scriptContext == null) {
                throw new UnsupportedOperationException("unsupported script context name [" + name + "]");
            }
            return scriptContext;
        }

        static class ContextSetup implements Writeable, ToXContentObject {

            private static final ParseField INDEX_FIELD = new ParseField("index");
            private static final ParseField DOCUMENT_FIELD = new ParseField("document");
            private static final ParseField QUERY_FIELD = new ParseField("query");
            private static final ConstructingObjectParser<ContextSetup, Void> PARSER = new ConstructingObjectParser<>(
                "execute_script_context",
                args -> new ContextSetup((String) args[0], (BytesReference) args[1], (QueryBuilder) args[2])
            );

            static {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_FIELD);
                PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                    try (XContentBuilder b = XContentBuilder.builder(p.contentType().xContent())) {
                        b.copyCurrentStructure(p);
                        return BytesReference.bytes(b);
                    }
                }, DOCUMENT_FIELD);
                PARSER.declareObject(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
                    QUERY_FIELD
                );
            }

            private final String index;
            private final BytesReference document;
            private final QueryBuilder query;

            private XContentType xContentType;

            static ContextSetup parse(XContentParser parser, Void context) throws IOException {
                ContextSetup contextSetup = PARSER.parse(parser, null);
                contextSetup.setXContentType(parser.contentType());
                return contextSetup;
            }

            ContextSetup(String index, BytesReference document, QueryBuilder query) {
                this.index = index;
                this.document = document;
                this.query = query;
            }

            ContextSetup(StreamInput in) throws IOException {
                index = in.readOptionalString();
                document = in.readOptionalBytesReference();
                String optionalXContentType = in.readOptionalString();
                if (optionalXContentType != null) {
                    this.xContentType = XContentType.fromMediaType(optionalXContentType);
                }
                query = in.readOptionalNamedWriteable(QueryBuilder.class);
            }

            public String getIndex() {
                return index;
            }

            public XContentType getXContentType() {
                return xContentType;
            }

            public void setXContentType(XContentType xContentType) {
                this.xContentType = xContentType;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ContextSetup that = (ContextSetup) o;
                return Objects.equals(index, that.index)
                    && Objects.equals(document, that.document)
                    && Objects.equals(query, that.query)
                    && Objects.equals(xContentType, that.xContentType);
            }

            @Override
            public int hashCode() {
                return Objects.hash(index, document, query, xContentType);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(index);
                out.writeOptionalBytesReference(document);
                out.writeOptionalString(xContentType != null ? xContentType.mediaTypeWithoutParameters() : null);
                out.writeOptionalNamedWriteable(query);
            }

            @Override
            public String toString() {
                return "ContextSetup{"
                    + ", index='"
                    + index
                    + '\''
                    + ", document="
                    + document
                    + ", query="
                    + query
                    + ", xContentType="
                    + xContentType
                    + '}';
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                {
                    if (index != null) {
                        builder.field(INDEX_FIELD.getPreferredName(), index);
                    }
                    if (document != null) {
                        builder.field(DOCUMENT_FIELD.getPreferredName());
                        try (
                            XContentParser parser = XContentHelper.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                document,
                                xContentType
                            )
                        ) {
                            builder.generator().copyCurrentStructure(parser);
                        }
                    }
                    if (query != null) {
                        builder.field(QUERY_FIELD.getPreferredName(), query);
                    }
                }
                builder.endObject();
                return builder;
            }

        }

        private final Script script;
        private final ScriptContext<?> context;
        private final ContextSetup contextSetup;

        static Request parse(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        Request(Script script, String scriptContextName, ContextSetup setup) {
            this.script = Objects.requireNonNull(script);
            this.context = scriptContextName != null ? fromScriptContextName(scriptContextName) : PainlessTestScript.CONTEXT;
            if (setup != null) {
                this.contextSetup = setup;
                index(contextSetup.index);
            } else {
                contextSetup = null;
            }
        }

        Request(StreamInput in) throws IOException {
            super(in);
            script = new Script(in);
            context = fromScriptContextName(in.readString());
            contextSetup = in.readOptionalWriteable(ContextSetup::new);
        }

        public Script getScript() {
            return script;
        }

        public ContextSetup getContextSetup() {
            return contextSetup;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            if (needDocumentAndIndex(context)) {
                if (contextSetup.index == null) {
                    validationException = addValidationError("index is a required parameter for current context", validationException);
                }
                if (contextSetup.document == null) {
                    validationException = addValidationError("document is a required parameter for current context", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
            out.writeString(context.name);
            out.writeOptionalWriteable(contextSetup);
        }

        // For testing only:
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.field(CONTEXT_FIELD.getPreferredName(), context.name);
            if (contextSetup != null) {
                builder.field(CONTEXT_SETUP_FIELD.getPreferredName(), contextSetup);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(script, request.script)
                && Objects.equals(context, request.context)
                && Objects.equals(contextSetup, request.contextSetup);
        }

        @Override
        public int hashCode() {
            return Objects.hash(script, context, contextSetup);
        }

        @Override
        public String toString() {
            return "Request{" + "script=" + script + "context=" + context + ", contextSetup=" + contextSetup + '}';
        }

        static boolean needDocumentAndIndex(ScriptContext<?> scriptContext) {
            return scriptContext == FilterScript.CONTEXT || scriptContext == ScoreScript.CONTEXT;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Object result;

        Response(Object result) {
            this.result = result;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            result = in.readGenericValue();
        }

        public Object getResult() {
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", result);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final ScriptService scriptService;
        private final IndicesService indicesServices;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ScriptService scriptService,
            ClusterService clusterService,
            IndicesService indicesServices
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                // Forking a thread here, because only light weight operations should happen on network thread and
                // Creating a in-memory index is not light weight
                // TODO: is MANAGEMENT TP the right TP? Right now this is an admin api (see action name).
                Request::new,
                ThreadPool.Names.MANAGEMENT
            );
            this.scriptService = scriptService;
            this.indicesServices = indicesServices;
        }

        @Override
        protected Writeable.Reader<Response> getResponseReader() {
            return Response::new;
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
            if (request.concreteIndex() != null) {
                return super.checkRequestBlock(state, request);
            }
            return null;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return request.contextSetup != null && request.contextSetup.getIndex() != null;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            if (request.concreteIndex() == null) {
                return null;
            }
            return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexService indexService;
            if (request.contextSetup != null && request.contextSetup.getIndex() != null) {
                ClusterState clusterState = clusterService.state();
                IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
                String indexExpression = request.contextSetup.index;
                Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions, indexExpression);
                if (concreteIndices.length != 1) {
                    throw new IllegalArgumentException("[" + indexExpression + "] does not resolve to a single index");
                }
                Index concreteIndex = concreteIndices[0];
                indexService = indicesServices.indexServiceSafe(concreteIndex);
            } else {
                indexService = null;
            }
            return innerShardOperation(request, scriptService, indexService);
        }

        static Response innerShardOperation(Request request, ScriptService scriptService, IndexService indexService) throws IOException {
            final ScriptContext<?> scriptContext = request.context;
            if (scriptContext == PainlessTestScript.CONTEXT) {
                PainlessTestScript.Factory factory = scriptService.compile(request.script, PainlessTestScript.CONTEXT);
                PainlessTestScript painlessTestScript = factory.newInstance(request.script.getParams());
                String result = Objects.toString(painlessTestScript.execute());
                return new Response(result);
            } else if (scriptContext == FilterScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    FilterScript.Factory factory = scriptService.compile(request.script, FilterScript.CONTEXT);
                    SearchLookup lookup = context.lookup();
                    FilterScript.LeafFactory leafFactory = factory.newFactory(request.getScript().getParams(), lookup);
                    FilterScript filterScript = leafFactory.newInstance(new DocValuesDocReader(lookup, leafReaderContext));
                    filterScript.setDocument(0);
                    boolean result = filterScript.execute();
                    return new Response(result);
                }, indexService);
            } else if (scriptContext == ScoreScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    ScoreScript.Factory factory = scriptService.compile(request.script, ScoreScript.CONTEXT);
                    SearchLookup lookup = context.lookup();
                    ScoreScript.LeafFactory leafFactory = factory.newFactory(request.getScript().getParams(), lookup);
                    ScoreScript scoreScript = leafFactory.newInstance(new DocValuesDocReader(lookup, leafReaderContext));
                    scoreScript.setDocument(0);

                    if (request.contextSetup.query != null) {
                        Query luceneQuery = request.contextSetup.query.rewrite(context).toQuery(context);
                        IndexSearcher indexSearcher = new IndexSearcher(leafReaderContext.reader());
                        luceneQuery = indexSearcher.rewrite(luceneQuery);
                        Weight weight = indexSearcher.createWeight(luceneQuery, ScoreMode.COMPLETE, 1f);
                        Scorer scorer = weight.scorer(indexSearcher.getIndexReader().leaves().get(0));
                        // Consume the first (and only) match.
                        int docID = scorer.iterator().nextDoc();
                        assert docID == scorer.docID();
                        scoreScript.setScorer(scorer);
                    }

                    double result = scoreScript.execute(null);
                    return new Response(result);
                }, indexService);
            } else if (scriptContext == BooleanFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    BooleanFieldScript.Factory factory = scriptService.compile(request.script, BooleanFieldScript.CONTEXT);
                    BooleanFieldScript.LeafFactory leafFactory = factory.newFactory(
                        BooleanFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    BooleanFieldScript booleanFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<Boolean> booleans = new ArrayList<>();
                    booleanFieldScript.runForDoc(0, booleans::add);
                    return new Response(booleans);
                }, indexService);
            } else if (scriptContext == DateFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    DateFieldScript.Factory factory = scriptService.compile(request.script, DateFieldScript.CONTEXT);
                    DateFieldScript.LeafFactory leafFactory = factory.newFactory(
                        DateFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        OnScriptError.FAIL
                    );
                    DateFieldScript dateFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<String> dates = new ArrayList<>();
                    dateFieldScript.runForDoc(0, d -> dates.add(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(d)));
                    return new Response(dates);
                }, indexService);
            } else if (scriptContext == DoubleFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    DoubleFieldScript.Factory factory = scriptService.compile(request.script, DoubleFieldScript.CONTEXT);
                    DoubleFieldScript.LeafFactory leafFactory = factory.newFactory(
                        DoubleFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    DoubleFieldScript doubleFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<Double> doubles = new ArrayList<>();
                    doubleFieldScript.runForDoc(0, doubles::add);
                    return new Response(doubles);
                }, indexService);
            } else if (scriptContext == GeoPointFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    GeoPointFieldScript.Factory factory = scriptService.compile(request.script, GeoPointFieldScript.CONTEXT);
                    GeoPointFieldScript.LeafFactory leafFactory = factory.newFactory(
                        GeoPointFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    GeoPointFieldScript geoPointFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<GeoPoint> points = new ArrayList<>();
                    geoPointFieldScript.runForDoc(0, gp -> points.add(new GeoPoint(gp)));
                    // convert geo points to the standard format of the fields api
                    Function<List<GeoPoint>, List<Object>> format = GeometryFormatterFactory.getFormatter(
                        GeometryFormatterFactory.GEOJSON,
                        p -> new Point(p.lon(), p.lat())
                    );
                    return new Response(format.apply(points));
                }, indexService);
            } else if (scriptContext == IpFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    IpFieldScript.Factory factory = scriptService.compile(request.script, IpFieldScript.CONTEXT);
                    IpFieldScript.LeafFactory leafFactory = factory.newFactory(
                        IpFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    IpFieldScript ipFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<String> ips = new ArrayList<>();
                    ipFieldScript.runForDoc(0, ip -> {
                        if (ip == null) {
                            ips.add(null);
                        } else {
                            ips.add(NetworkAddress.format(ip));
                        }
                    });
                    return new Response(ips);
                }, indexService);
            } else if (scriptContext == LongFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    LongFieldScript.Factory factory = scriptService.compile(request.script, LongFieldScript.CONTEXT);
                    LongFieldScript.LeafFactory leafFactory = factory.newFactory(
                        LongFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    LongFieldScript longFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<Long> longs = new ArrayList<>();
                    longFieldScript.runForDoc(0, longs::add);
                    return new Response(longs);
                }, indexService);
            } else if (scriptContext == StringFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    StringFieldScript.Factory factory = scriptService.compile(request.script, StringFieldScript.CONTEXT);
                    StringFieldScript.LeafFactory leafFactory = factory.newFactory(
                        StringFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    StringFieldScript stringFieldScript = leafFactory.newInstance(leafReaderContext);
                    List<String> keywords = new ArrayList<>();
                    stringFieldScript.runForDoc(0, keywords::add);
                    return new Response(keywords);
                }, indexService);
            } else if (scriptContext == CompositeFieldScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    CompositeFieldScript.Factory factory = scriptService.compile(request.script, CompositeFieldScript.CONTEXT);
                    CompositeFieldScript.LeafFactory leafFactory = factory.newFactory(
                        CompositeFieldScript.CONTEXT.name,
                        request.getScript().getParams(),
                        context.lookup(),
                        OnScriptError.FAIL
                    );
                    CompositeFieldScript compositeFieldScript = leafFactory.newInstance(leafReaderContext);
                    compositeFieldScript.runForDoc(0);
                    return new Response(compositeFieldScript.getFieldValues());
                }, indexService);
            } else {
                throw new UnsupportedOperationException("unsupported context [" + scriptContext.name + "]");
            }
        }

        private static Response prepareRamIndex(
            Request request,
            CheckedBiFunction<SearchExecutionContext, LeafReaderContext, Response, IOException> handler,
            IndexService indexService
        ) throws IOException {
            Analyzer defaultAnalyzer = indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();

            try (Directory directory = new ByteBuffersDirectory()) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(defaultAnalyzer))) {
                    BytesReference document = request.contextSetup.document;
                    XContentType xContentType = request.contextSetup.xContentType;
                    SourceToParse sourceToParse = new SourceToParse("_id", document, xContentType);
                    DocumentMapper documentMapper = indexService.mapperService().documentMapper();
                    if (documentMapper == null) {
                        documentMapper = DocumentMapper.createEmpty(indexService.mapperService());
                    }
                    // Note that we are not doing anything with dynamic mapping updates, hence fields that are not mapped but are present
                    // in the sample doc are not accessible from the script through doc['field'].
                    // This is a problem especially for indices that have no mappings, as no fields will be accessible, neither through doc
                    // nor _source (if there are no mappings there are no metadata fields).
                    ParsedDocument parsedDocument = documentMapper.parse(sourceToParse);
                    indexWriter.addDocuments(parsedDocument.docs());
                    try (IndexReader indexReader = DirectoryReader.open(indexWriter)) {
                        final IndexSearcher searcher = new IndexSearcher(indexReader);
                        searcher.setQueryCache(null);
                        final long absoluteStartMillis = System.currentTimeMillis();
                        SearchExecutionContext context = indexService.newSearchExecutionContext(
                            0,
                            0,
                            searcher,
                            () -> absoluteStartMillis,
                            null,
                            emptyMap()
                        );
                        return handler.apply(context, indexReader.leaves().get(0));
                    }
                }
            }
        }
    }

    @ServerlessScope(Scope.PUBLIC)
    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return List.of(new Route(GET, "/_scripts/painless/_execute"), new Route(POST, "/_scripts/painless/_execute"));
        }

        @Override
        public String getName() {
            return "_scripts_painless_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }

}
