/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetStoredScriptResponseTests extends AbstractXContentSerializingTestCase<GetStoredScriptResponse> {

    private static final ConstructingObjectParser<GetStoredScriptResponse, String> PARSER = new ConstructingObjectParser<>(
        "GetStoredScriptResponse",
        true,
        (a, c) -> {
            String id = (String) a[0];
            boolean found = (Boolean) a[1];
            StoredScriptSource scriptSource = (StoredScriptSource) a[2];
            return found ? new GetStoredScriptResponse(id, scriptSource) : new GetStoredScriptResponse(id, null);
        }
    );

    static {
        PARSER.declareField(constructorArg(), (p, c) -> p.text(), GetStoredScriptResponse._ID_PARSE_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> p.booleanValue(),
            GetStoredScriptResponse.FOUND_PARSE_FIELD,
            ObjectParser.ValueType.BOOLEAN
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> StoredScriptSource.fromXContent(p, true),
            GetStoredScriptResponse.SCRIPT,
            ObjectParser.ValueType.OBJECT
        );
    }

    @Override
    protected GetStoredScriptResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected GetStoredScriptResponse createTestInstance() {
        return new GetStoredScriptResponse(randomAlphaOfLengthBetween(1, 10), randomScriptSource());
    }

    @Override
    protected GetStoredScriptResponse mutateInstance(GetStoredScriptResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<GetStoredScriptResponse> instanceReader() {
        return GetStoredScriptResponse::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return s -> "script.options".equals(s);
    }

    private static StoredScriptSource randomScriptSource() {
        final String lang = randomFrom("lang", "painless", "mustache");
        final String source = randomAlphaOfLengthBetween(1, 10);
        final Map<String, String> options = randomBoolean()
            ? Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType())
            : Collections.emptyMap();
        return new StoredScriptSource(lang, source, options);
    }
}
