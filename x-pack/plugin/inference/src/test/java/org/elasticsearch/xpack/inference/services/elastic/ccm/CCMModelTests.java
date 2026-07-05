/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

public class CCMModelTests extends AbstractBWCWireSerializationTestCase<CCMModel> {

    public void testToXContent() throws IOException {
        var model = new CCMModel(new SecureString("secret".toCharArray()));
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        model.toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
                {
                    "api_key": "secret"
                }
            """)));
    }

    public void testFromXContentBytes() throws IOException {
        String json = """
            {
                "api_key": "test_key"
            }
            """;
        var model = CCMModel.fromXContentBytes(new BytesArray(json));

        assertThat(model.apiKey().toString(), is("test_key"));
    }

    public void testFromXContentBytes_ThrowsException_WhenApiKeyMissing() {
        String json = """
            {
            }
            """;
        var exception = expectThrows(IllegalArgumentException.class, () -> CCMModel.fromXContentBytes(new BytesArray(json)));
        assertThat(exception.getMessage(), containsString("Required [api_key]"));
    }

    @Override
    protected CCMModel mutateInstanceForVersion(CCMModel instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<CCMModel> instanceReader() {
        return CCMModel::new;
    }

    @Override
    protected CCMModel createTestInstance() {
        return new CCMModel(new SecureString(randomAlphaOfLength(10).toCharArray()));
    }

    @Override
    protected CCMModel mutateInstance(CCMModel instance) throws IOException {
        var originalString = instance.apiKey().toString();
        return new CCMModel(new SecureString((originalString + "modified").toCharArray()));
    }
}
