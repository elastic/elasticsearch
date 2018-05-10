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
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class PainlessExecuteAction extends Action<PainlessExecuteAction.Request, PainlessExecuteAction.Response,
    PainlessExecuteAction.RequestBuilder> {

    static final PainlessExecuteAction INSTANCE = new PainlessExecuteAction();
    private static final String NAME = "cluster:admin/scripts/painless/execute";

    private PainlessExecuteAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContent {

        private static final ParseField SCRIPT_FIELD = new ParseField("script");
        private static final ParseField CONTEXT_FIELD = new ParseField("context");
        private static final ParseField INDEX_FIELD = new ParseField("index");
        private static final ParseField DOCUMENT_FIELD = new ParseField("document");
        private static final ConstructingObjectParser<Request, ParseContext> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request", args -> new Request((Script) args[0], (SupportedContext) args[1]));

        private static class ParseContext {

            private String index;
            private BytesReference document;

        }

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Script.parse(p), SCRIPT_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                // For now only accept an empty json object:
                XContentParser.Token token = p.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String contextType = p.currentName();
                SupportedContext supportedContext = SupportedContext.valueOf(contextType.toUpperCase(Locale.ROOT));
                if (supportedContext == SupportedContext.PAINLESS_TEST) {
                    token = p.nextToken();
                    assert token == XContentParser.Token.START_OBJECT;
                    token = p.nextToken();
                    assert token == XContentParser.Token.END_OBJECT;
                    token = p.nextToken();
                    assert token == XContentParser.Token.END_OBJECT;
                } else if (supportedContext == SupportedContext.FILTER_SCRIPT || supportedContext == SupportedContext.SEARCH_SCRIPT) {
                    for (token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                        if (token.isValue()) {
                            if (INDEX_FIELD.match(p.currentName(), p.getDeprecationHandler())) {
                                c.index = p.textOrNull();
                            } else {
                                throw new IllegalStateException("Unexpected field name [" + p.currentName() + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if (DOCUMENT_FIELD.match(p.currentName(), p.getDeprecationHandler())) {
                                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                                    builder.copyCurrentStructure(p);
                                    builder.flush();
                                    c.document = BytesReference.bytes(builder);
                                }
                            }
                        } else if (token == XContentParser.Token.FIELD_NAME) {
                            // ignore
                        } else {
                            throw new IllegalStateException("Unexpected token [" + token + "]");
                        }
                    }
                } else {
                    assert false : "Unsupported context";
                }
                return supportedContext;
            }, CONTEXT_FIELD);
        }

        private Script script;
        private SupportedContext context;

        private String index;
        private BytesReference document;
        private XContentType xContentType;

        static Request parse(XContentParser parser) throws IOException {
            ParseContext parseContext = new ParseContext();
            Request request = PARSER.parse(parser, parseContext);
            request.setIndex(parseContext.index);
            request.setDocument(parseContext.document);
            request.setXContentType(parser.contentType());
            return request;
        }

        Request(Script script, SupportedContext context) {
            this.script = Objects.requireNonNull(script);
            this.context = context != null ? context : SupportedContext.PAINLESS_TEST;
        }

        Request() {
        }

        public Script getScript() {
            return script;
        }

        public SupportedContext getContext() {
            return context;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public BytesReference getDocument() {
            return document;
        }

        public void setDocument(BytesReference document) {
            this.document = document;
        }

        public XContentType getXContentType() {
            return xContentType;
        }

        public void setXContentType(XContentType xContentType) {
            this.xContentType = xContentType;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (script.getType() != ScriptType.INLINE) {
                validationException = addValidationError("only inline scripts are supported", validationException);
            }
            if (context == SupportedContext.FILTER_SCRIPT || context == SupportedContext.SEARCH_SCRIPT) {
                if (index == null) {
                    validationException = addValidationError("index is a required parameter for current context", validationException);
                }
                if (document == null) {
                    validationException = addValidationError("document is a required parameter for current context", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            script = new Script(in);
            context = SupportedContext.fromId(in.readByte());
            if (context == SupportedContext.FILTER_SCRIPT) {
                index = in.readString();
                document = in.readBytesReference();
                xContentType = XContentType.fromMediaType(in.readString());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            script.writeTo(out);
            out.writeByte(context.id);
            out.writeOptionalString(index);
            if (context == SupportedContext.FILTER_SCRIPT) {
                out.writeString(index);
                out.writeBytesReference(document);
                out.writeString(xContentType.mediaType());
            }
        }

        // For testing only:
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.startObject(CONTEXT_FIELD.getPreferredName());
            {
                builder.startObject(context.name());
                if (context == SupportedContext.FILTER_SCRIPT) {
                    builder.field(INDEX_FIELD.getPreferredName(), index);
                    try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE, document)) {
                        parser.nextToken();
                        builder.generator().copyCurrentStructure(parser);
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(script, request.script) &&
                context == request.context &&
                Objects.equals(index, request.index) &&
                Objects.equals(document, request.document);
        }

        @Override
        public int hashCode() {
            return Objects.hash(script, context, index, document);
        }

        public enum SupportedContext {

            PAINLESS_TEST((byte) 0),
            FILTER_SCRIPT((byte) 1),
            SEARCH_SCRIPT((byte) 2);

            private final byte id;

            SupportedContext(byte id) {
                this.id = id;
            }

            public static SupportedContext fromId(byte id) {
                switch (id) {
                    case 0:
                        return PAINLESS_TEST;
                    case 1:
                        return FILTER_SCRIPT;
                    case 2:
                        return SEARCH_SCRIPT;
                    default:
                        throw new IllegalArgumentException("unknown context [" + id + "]");
                }
            }
        }

    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

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

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ScriptService scriptService;
        private final ClusterService clusterService;
        private final IndicesService indicesServices;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ScriptService scriptService, ClusterService clusterService, IndicesService indicesServices) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.scriptService = scriptService;
            this.clusterService = clusterService;
            this.indicesServices = indicesServices;
        }
        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            switch (request.context) {
                case PAINLESS_TEST:
                    {
                        PainlessTestScript.Factory factory = scriptService.compile(request.script, PainlessTestScript.CONTEXT);
                        PainlessTestScript painlessTestScript = factory.newInstance(request.script.getParams());
                        String result = Objects.toString(painlessTestScript.execute());
                        listener.onResponse(new Response(result));
                    }
                    break;
                case FILTER_SCRIPT:
                    {
                        indexAndOpenIndexReader(request, (context, leafReaderContext) -> {
                            FilterScript.Factory factory = scriptService.compile(request.script, FilterScript.CONTEXT);
                            FilterScript.LeafFactory leafFactory =
                                factory.newFactory(request.getScript().getParams(), context.lookup());
                            FilterScript filterScript = leafFactory.newInstance(leafReaderContext);
                            boolean result = filterScript.execute();
                            listener.onResponse(new Response(result));
                        });
                    }
                    break;
                case SEARCH_SCRIPT:
                    {
                        indexAndOpenIndexReader(request, (context, leafReaderContext) -> {
                            SearchScript.Factory factory = scriptService.compile(request.script, SearchScript.CONTEXT);
                            SearchScript.LeafFactory leafFactory =
                                factory.newFactory(request.getScript().getParams(), context.lookup());
                            SearchScript searchScript = leafFactory.newInstance(leafReaderContext);
                            Object result = searchScript.run();
                            listener.onResponse(new Response(result));
                        });
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported context [" + request.context + "]");
            }
        }

        private void indexAndOpenIndexReader(Request request,
                                             CheckedBiConsumer<QueryShardContext, LeafReaderContext, IOException> handler) {
            IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
            Index[] concreteIndices =
                indexNameExpressionResolver.concreteIndices(clusterService.state(), indicesOptions, request.index);
            if (concreteIndices.length != 1) {
                throw new IllegalArgumentException("[" + request.index + "] does not resolve to a single index");
            }
            Index concreteIndex = concreteIndices[0];
            IndexService indexService = indicesServices.indexServiceSafe(concreteIndex);
            Analyzer defaultAnalyzer = indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();

            try (RAMDirectory ramDirectory = new RAMDirectory()) {
                try (IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(defaultAnalyzer))) {
                    String index = concreteIndex.getName();
                    String type = indexService.mapperService().documentMapper().type();
                    SourceToParse sourceToParse = SourceToParse.source(index, type, "_id", request.document, request.xContentType);
                    ParsedDocument parsedDocument = indexService.mapperService().documentMapper().parse(sourceToParse);
                    indexWriter.addDocuments(parsedDocument.docs());
                    try (IndexReader indexReader = DirectoryReader.open(indexWriter)) {
                        final long absoluteStartMillis = System.currentTimeMillis();
                        QueryShardContext context =
                            indexService.newQueryShardContext(0, indexReader, () -> absoluteStartMillis, null);
                        handler.accept(context, indexReader.leaves().get(0));
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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
