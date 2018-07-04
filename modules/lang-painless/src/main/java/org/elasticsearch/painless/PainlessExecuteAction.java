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
package org.elasticsearch.painless;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class PainlessExecuteAction extends Action<PainlessExecuteAction.Response> {

    static final PainlessExecuteAction INSTANCE = new PainlessExecuteAction();
    private static final String NAME = "cluster:admin/scripts/painless/execute";

    private PainlessExecuteAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> implements ToXContent {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request", args -> new Request((Script) args[0], (ExecuteScriptContext) args[1]));

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                XContentParser.Token token = p.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String contextType = p.currentName();
                ScriptContext<?> scriptContext = fromScriptContextName(contextType.toLowerCase(Locale.ROOT));
                ExecuteScriptContext context = ExecuteScriptContext.PARSER.parse(p, c);
                context.scriptContext = scriptContext;
                context.xContentType = p.contentType().xContent().type();
                token = p.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return context;
            }, CONTEXT_FIELD);
        }

        static final Map<String, ScriptContext<?>> SUPPORTED_CONTEXTS;

        static {
            Map<String, ScriptContext<?>> supportedContexts = new HashMap<>();
            supportedContexts.put("painless_test", PainlessTestScript.CONTEXT);
            supportedContexts.put("filter", FilterScript.CONTEXT);
            supportedContexts.put("score", ScoreScript.CONTEXT);
            SUPPORTED_CONTEXTS = Collections.unmodifiableMap(supportedContexts);
        }

        static ScriptContext<?> fromScriptContextName(String name) {
            ScriptContext<?> scriptContext = SUPPORTED_CONTEXTS.get(name);
            if (scriptContext == null) {
                throw new UnsupportedOperationException("unsupported script context name [" + name + "]");
            }
            return scriptContext;
        }

        static class ExecuteScriptContext implements Writeable, ToXContentObject {

            private static final ParseField INDEX_FIELD = new ParseField("index");
            private static final ParseField DOCUMENT_FIELD = new ParseField("document");
            private static final ParseField QUERY_FIELD = new ParseField("query");
            private static final ObjectParser<ExecuteScriptContext, Void> PARSER =
                new ObjectParser<>("execute_script_context", ExecuteScriptContext::new);

            static {
                PARSER.declareString(ExecuteScriptContext::setIndex, INDEX_FIELD);
                PARSER.declareObject(ExecuteScriptContext::setDocument, (p, c) -> {
                    try (XContentBuilder b = XContentBuilder.builder(p.contentType().xContent())) {
                        b.copyCurrentStructure(p);
                        return BytesReference.bytes(b);
                    }
                }, DOCUMENT_FIELD);
                PARSER.declareObject(ExecuteScriptContext::setQuery, (p, c) ->
                    AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY_FIELD);
            }

            private ScriptContext<?> scriptContext = PainlessTestScript.CONTEXT;
            private String index;
            private BytesReference document;
            private QueryBuilder query;

            private XContentType xContentType;

            ExecuteScriptContext(StreamInput in) throws IOException {
                scriptContext = fromScriptContextName(in.readString());
                index = in.readOptionalString();
                document = in.readOptionalBytesReference();
                String xContentType = in.readOptionalString();
                if (xContentType  != null) {
                    this.xContentType = XContentType.fromMediaType(xContentType);
                }
                query = in.readOptionalNamedWriteable(QueryBuilder.class);
            }

            ExecuteScriptContext() {
            }

            void setIndex(String index) {
                this.index = index;
            }

            void setDocument(BytesReference document) {
                this.document = document;
            }

            void setQuery(QueryBuilder query) {
                this.query = query;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ExecuteScriptContext that = (ExecuteScriptContext) o;
                return Objects.equals(scriptContext, that.scriptContext) &&
                    Objects.equals(index, that.index) &&
                    Objects.equals(document, that.document) &&
                    Objects.equals(query, that.query);
            }

            @Override
            public int hashCode() {
                return Objects.hash(scriptContext, index, document, query);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(scriptContext.name);
                out.writeOptionalString(index);
                out.writeOptionalBytesReference(document);
                out.writeOptionalString(xContentType != null ? xContentType.mediaType(): null);
                out.writeOptionalNamedWriteable(query);
            }

            @Override
            public String toString() {
                return "ExecuteScriptContext{" +
                    "scriptContext=" + scriptContext.name +
                    ", index='" + index + '\'' +
                    ", document=" + document +
                    ", query=" + query +
                    ", xContentType=" + xContentType +
                    '}';
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                {
                    builder.startObject(scriptContext.name);
                    {
                        if (index != null) {
                            builder.field(INDEX_FIELD.getPreferredName(), index);
                        }
                        if (document != null) {
                            builder.field(DOCUMENT_FIELD.getPreferredName());
                            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE, document, xContentType)) {
                                builder.generator().copyCurrentStructure(parser);
                            }
                        }
                        if (query != null) {
                            builder.field(QUERY_FIELD.getPreferredName(), query);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
                return builder;
            }

        }

        private Script script;
        private ExecuteScriptContext executeScriptContext = new ExecuteScriptContext();

        static Request parse(XContentParser parser) throws IOException {
            Request request = PARSER.parse(parser, null);
            request.setXContentType(parser.contentType());
            return request;
        }

        Request(Script script, ExecuteScriptContext context) {
            this.script = Objects.requireNonNull(script);
            if (context != null) {
                this.executeScriptContext = context;
                index(executeScriptContext.index);
            }
        }

        Request(Script script, String scriptContextName) {
            this.script = Objects.requireNonNull(script);
            if (scriptContextName != null) {
                this.executeScriptContext = new ExecuteScriptContext();
                this.executeScriptContext.scriptContext = fromScriptContextName(scriptContextName);
            }
        }

        Request() {
        }

        public Script getScript() {
            return script;
        }

        public String getIndex() {
            return executeScriptContext.index;
        }

        public void setIndex(String index) {
            this.executeScriptContext.index = index;
            index(executeScriptContext.index);
        }

        public BytesReference getDocument() {
            return executeScriptContext.document;
        }

        public void setDocument(BytesReference document) {
            this.executeScriptContext.document = document;
        }

        public XContentType getXContentType() {
            return executeScriptContext.xContentType;
        }

        public void setXContentType(XContentType xContentType) {
            this.executeScriptContext.xContentType = xContentType;
        }

        public QueryBuilder getQuery() {
            return executeScriptContext.query;
        }

        public void setQuery(QueryBuilder query) {
            this.executeScriptContext.query = query;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            if (needDocumentAndIndex(executeScriptContext.scriptContext)) {
                if (executeScriptContext.index == null) {
                    validationException = addValidationError("index is a required parameter for current context", validationException);
                }
                if (executeScriptContext.document == null) {
                    validationException = addValidationError("document is a required parameter for current context", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            script = new Script(in);
            if (in.getVersion().onOrBefore(Version.V_6_4_0)) {
                byte scriptContextId = in.readByte();
                assert scriptContextId == 0;
            } else {
                executeScriptContext = new ExecuteScriptContext(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
            if (out.getVersion().onOrBefore(Version.V_6_4_0)) {
                out.writeByte((byte) 0);
            } else {
                executeScriptContext.writeTo(out);
            }
        }

        // For testing only:
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.field(CONTEXT_FIELD.getPreferredName(), executeScriptContext);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(script, request.script) &&
                Objects.equals(executeScriptContext, request.executeScriptContext);
        }

        @Override
        public int hashCode() {
            return Objects.hash(script, executeScriptContext);
        }

        @Override
        public String toString() {
            return "Request{" +
                "script=" + script +
                ", executeScriptContext=" + executeScriptContext +
                '}';
        }

        static boolean needDocumentAndIndex(ScriptContext<?> scriptContext) {
            return scriptContext == FilterScript.CONTEXT || scriptContext == ScoreScript.CONTEXT;
        }

    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Object result;

        Response() {}

        Response(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return result;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            result = in.readGenericValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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

    public abstract static class PainlessTestScript {

        private final Map<String, Object> params;

        public PainlessTestScript(Map<String, Object> params) {
            this.params = params;
        }

        /** Return the parameters for this script. */
        public Map<String, Object> getParams() {
            return params;
        }

        public abstract Object execute();

        public interface Factory {

            PainlessTestScript newInstance(Map<String, Object> params);

        }

        public static final String[] PARAMETERS = {};
        public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("painless_test", Factory.class);

    }

    public static class TransportAction extends TransportSingleShardAction<Request, Response> {

        private final ScriptService scriptService;
        private final IndicesService indicesServices;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ScriptService scriptService, ClusterService clusterService, IndicesService indicesServices) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                // Forking a thread here, because only light weight operations should happen on network thread and
                // Creating a in-memory index is not light weight
                // TODO: is MANAGEMENT TP the right TP? Right now this is an admin api (see action name).
                Request::new, ThreadPool.Names.MANAGEMENT);
            this.scriptService = scriptService;
            this.indicesServices = indicesServices;
        }

        @Override
        protected Response newResponse() {
            return new Response();
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
            return request.getIndex() != null;
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
            if (request.getIndex() != null) {
                ClusterState clusterState = clusterService.state();
                IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
                String indexExpression = request.executeScriptContext.index;
                Index[] concreteIndices =
                    indexNameExpressionResolver.concreteIndices(clusterState, indicesOptions, indexExpression);
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
            final ScriptContext<?> scriptContext = request.executeScriptContext.scriptContext;
            if (scriptContext == PainlessTestScript.CONTEXT) {
                PainlessTestScript.Factory factory = scriptService.compile(request.script, PainlessTestScript.CONTEXT);
                PainlessTestScript painlessTestScript = factory.newInstance(request.script.getParams());
                String result = Objects.toString(painlessTestScript.execute());
                return new Response(result);
            } else if (scriptContext == FilterScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    FilterScript.Factory factory = scriptService.compile(request.script, FilterScript.CONTEXT);
                    FilterScript.LeafFactory leafFactory =
                        factory.newFactory(request.getScript().getParams(), context.lookup());
                    FilterScript filterScript = leafFactory.newInstance(leafReaderContext);
                    filterScript.setDocument(0);
                    boolean result = filterScript.execute();
                    return new Response(result);
                }, indexService);
            } else if (scriptContext == ScoreScript.CONTEXT) {
                return prepareRamIndex(request, (context, leafReaderContext) -> {
                    ScoreScript.Factory factory = scriptService.compile(request.script, ScoreScript.CONTEXT);
                    ScoreScript.LeafFactory leafFactory =
                        factory.newFactory(request.getScript().getParams(), context.lookup());
                    ScoreScript scoreScript = leafFactory.newInstance(leafReaderContext);
                    scoreScript.setDocument(0);

                    if (request.executeScriptContext.query != null) {
                        Query luceneQuery = request.executeScriptContext.query.rewrite(context).toQuery(context);
                        IndexSearcher indexSearcher = new IndexSearcher(leafReaderContext.reader());
                        luceneQuery = indexSearcher.rewrite(luceneQuery);
                        Weight weight = indexSearcher.createWeight(luceneQuery, true, 1f);
                        Scorer scorer = weight.scorer(indexSearcher.getIndexReader().leaves().get(0));
                        // Consume the first (and only) match.
                        int docID = scorer.iterator().nextDoc();
                        assert docID == scorer.docID();
                        scoreScript.setScorer(scorer);
                    }

                    double result = scoreScript.execute();
                    return new Response(result);
                }, indexService);
            } else {
                throw new UnsupportedOperationException("unsupported context [" + scriptContext.name + "]");
            }
        }

        private static Response prepareRamIndex(Request request,
                                                CheckedBiFunction<QueryShardContext, LeafReaderContext, Response, IOException> handler,
                                                IndexService indexService) throws IOException {

            Analyzer defaultAnalyzer = indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();

            try (RAMDirectory ramDirectory = new RAMDirectory()) {
                try (IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(defaultAnalyzer))) {
                    String index = indexService.index().getName();
                    String type = indexService.mapperService().documentMapper().type();
                    BytesReference document = request.executeScriptContext.document;
                    XContentType xContentType = request.executeScriptContext.xContentType;
                    SourceToParse sourceToParse = SourceToParse.source(index, type, "_id", document, xContentType);
                    ParsedDocument parsedDocument = indexService.mapperService().documentMapper().parse(sourceToParse);
                    indexWriter.addDocuments(parsedDocument.docs());
                    try (IndexReader indexReader = DirectoryReader.open(indexWriter)) {
                        final long absoluteStartMillis = System.currentTimeMillis();
                        QueryShardContext context =
                            indexService.newQueryShardContext(0, indexReader, () -> absoluteStartMillis, null);
                        return handler.apply(context, indexReader.leaves().get(0));
                    }
                }
            }
        }
    }

    static class RestAction extends BaseRestHandler {

        RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(GET, "/_scripts/painless/_execute", this);
            controller.registerHandler(POST, "/_scripts/painless/_execute", this);
        }

        @Override
        public String getName() {
            return "_scripts_painless_execute";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            final Request request = Request.parse(restRequest.contentOrSourceParamParser());
            return channel -> client.executeLocally(INSTANCE, request, new RestBuilderListener<Response>(channel) {
                @Override
                public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(OK, builder);
                }
            });
        }
    }

}
