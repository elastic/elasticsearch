/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.rest.Stash;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransformAndSetSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseSingleValue() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ key: value }");

        TransformAndSetSection transformAndSet = TransformAndSetSection.parse(parser);
        assertThat(transformAndSet, notNullValue());
        assertThat(transformAndSet.getStash(), notNullValue());
        assertThat(transformAndSet.getStash().size(), equalTo(1));
        assertThat(transformAndSet.getStash().get("key"), equalTo("value"));
    }

    public void testParseMultipleValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ key1: value1, key2: value2 }");

        TransformAndSetSection transformAndSet = TransformAndSetSection.parse(parser);
        assertThat(transformAndSet, notNullValue());
        assertThat(transformAndSet.getStash(), notNullValue());
        assertThat(transformAndSet.getStash().size(), equalTo(2));
        assertThat(transformAndSet.getStash().get("key1"), equalTo("value1"));
        assertThat(transformAndSet.getStash().get("key2"), equalTo("value2"));
    }

    public void testTransformation() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ login_creds: \"#base64EncodeCredentials(id,api_key)\" }");

        TransformAndSetSection transformAndSet = TransformAndSetSection.parse(parser);
        assertThat(transformAndSet, notNullValue());
        assertThat(transformAndSet.getStash(), notNullValue());
        assertThat(transformAndSet.getStash().size(), equalTo(1));
        assertThat(transformAndSet.getStash().get("login_creds"), equalTo("#base64EncodeCredentials(id,api_key)"));

        ClientYamlTestExecutionContext executionContext = mock(ClientYamlTestExecutionContext.class);
        when(executionContext.response("id")).thenReturn("user");
        when(executionContext.response("api_key")).thenReturn("password");
        Stash stash = new Stash();
        when(executionContext.stash()).thenReturn(stash);
        transformAndSet.execute(executionContext);
        verify(executionContext).response("id");
        verify(executionContext).response("api_key");
        verify(executionContext).stash();
        assertThat(
            stash.getValue("$login_creds"),
            equalTo(Base64.getEncoder().encodeToString("user:password".getBytes(StandardCharsets.UTF_8)))
        );
        verifyNoMoreInteractions(executionContext);
    }

    public void testParseSetSectionNoValues() throws Exception {
        parser = createParser(YamlXContent.yamlXContent, "{ }");

        Exception e = expectThrows(ParsingException.class, () -> TransformAndSetSection.parse(parser));
        assertThat(e.getMessage(), is("transform_and_set section must set at least a value"));
    }
}
