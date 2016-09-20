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
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptMetaData.StoredScriptSource;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutStoredScriptAction extends BaseRestHandler {
    private static class StoredScriptSourceParserContext implements ParseFieldMatcherSupplier {

        private final ParseFieldMatcher parseFieldMatcher;

        StoredScriptSourceParserContext() {
            this.parseFieldMatcher = new ParseFieldMatcher(true);
        }

        @Override
        public ParseFieldMatcher getParseFieldMatcher() {
            return parseFieldMatcher;
        }
    }

    private static final ParseField parseScript  = new ParseField("script");
    private static final ParseField parseContext = new ParseField("context");
    private static final ParseField parseCode    = new ParseField("code");
    private static final ParseField parseLang    = new ParseField("lang");

    static final ConstructingObjectParser<StoredScriptSource, StoredScriptSourceParserContext> CONSTRUCTOR =
        new ConstructingObjectParser<>("StoredScriptSource", source -> new StoredScriptSource(
            (String)source[0], source[1] == null ? Script.DEFAULT_SCRIPT_LANG : (String)source[1], (String)source[2]));

    static {
        CONSTRUCTOR.declareString(optionalConstructorArg(), parseContext);
        CONSTRUCTOR.declareString(optionalConstructorArg(), parseLang);
        CONSTRUCTOR.declareString(constructorArg(), parseCode);
    }

    @Inject
    public RestPutStoredScriptAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(POST, "/_scripts/{id}", this);
        controller.registerHandler(PUT, "/_scripts/{id}", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
        StoredScriptSource source = parseStoredScript(request.content());
        PutStoredScriptRequest putRequest = new PutStoredScriptRequest(request.param("id"), source);
        client.admin().cluster().putStoredScript(putRequest, new AcknowledgedRestListener<>(channel));
    }

    private static StoredScriptSource parseStoredScript(BytesReference content) {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            if (parser.nextToken() != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected start object [{]");
            }

            if (parser.nextToken() == Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected [<code>]");
            }

            if (parser.currentToken() != Token.FIELD_NAME || !parseScript.getPreferredName().equals(parser.currentName())) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected [script]");
            }

            if (parser.nextToken() == Token.VALUE_STRING) {
                return new StoredScriptSource(null, Script.DEFAULT_SCRIPT_LANG, parser.text());
            } else if (parser.currentToken() == Token.START_OBJECT) {
                return CONSTRUCTOR.apply(parser, new StoredScriptSourceParserContext());
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected [<code>]");
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
