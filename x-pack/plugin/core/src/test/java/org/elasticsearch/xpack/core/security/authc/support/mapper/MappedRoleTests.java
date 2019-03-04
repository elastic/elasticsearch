/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.mapper.MappedRole.NamedRole;
import org.elasticsearch.xpack.core.security.authc.support.mapper.MappedRole.TemplateRole;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MappedRoleTests extends ESTestCase {

    public void testParseRoles() throws Exception {
        final String name = randomAlphaOfLengthBetween(3, 8);
        final MappedRole role1 = parse('"' + name + '"');
        assertThat(role1, Matchers.instanceOf(NamedRole.class));
        assertThat(((NamedRole) role1).getRoleName(), equalTo(name));

        final MappedRole role2 = parse("{ \"template\": { \"source\": \"_user_{{username}}\" } }");
        assertThat(role2, Matchers.instanceOf(TemplateRole.class));
        assertThat(((TemplateRole) role2).getTemplate().utf8ToString(), equalTo("{\"source\":\"_user_{{username}}\"}"));
        assertThat(((TemplateRole) role2).getFormat(), equalTo(TemplateRole.Format.STRING));

        final MappedRole role3 = parse("{ \"template\": \"{\\\"source\\\":\\\"{{#tojson}}groups{{/tojson}}\\\"}\", \"format\":\"json\" }");
        assertThat(role3, Matchers.instanceOf(TemplateRole.class));
        assertThat(((TemplateRole) role3).getTemplate().utf8ToString(),
            equalTo("{\"source\":\"{{#tojson}}groups{{/tojson}}\"}"));
        assertThat(((TemplateRole) role3).getFormat(), equalTo(TemplateRole.Format.JSON));
    }

    public void testToXContent() throws Exception {
        final String json1 = '"' + randomAlphaOfLengthBetween(3, 8) + '"';
        assertThat(Strings.toString(parse(json1)), equalTo(json1));

        final String json2 = "{" +
            "\"template\":\"{\\\"source\\\":\\\"" + randomAlphaOfLengthBetween(8, 24) + "\\\"}\"," +
            "\"format\":\"" + randomFrom(TemplateRole.Format.values()).formatName() + "\"" +
            "}";
        assertThat(Strings.toString(parse(json2)), equalTo(json2));
    }

    public void testSerializeNamedRole() throws Exception {
        trySerialize(new NamedRole(randomAlphaOfLengthBetween(3, 12)));
    }

    public void testSerializeTemplateRole() throws Exception {
        trySerialize(new TemplateRole(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(TemplateRole.Format.values())));
    }

    public void testEqualsAndHashCodeForNamedRole() throws Exception {
        tryEquals(new NamedRole(randomAlphaOfLengthBetween(3, 12)));
    }

    public void testEqualsAndHashCodeForTemplateRole() throws Exception {
        tryEquals(new TemplateRole(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(TemplateRole.Format.values())));
    }

    public void testEvaluateRoles() throws Exception {
        final ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        final ExpressionModel model = new ExpressionModel();
        model.defineField("username", "hulk");
        model.defineField("groups", Arrays.asList("avengers", "defenders", "panthenon"));

        final String name1 = randomAlphaOfLengthBetween(3, 12);
        assertThat(new NamedRole(name1).getRoleNames(scriptService, model), contains(name1));

        final TemplateRole userRole = new TemplateRole(new BytesArray("{ \"source\":\"_user_{{username}}\" }"), TemplateRole.Format.STRING);
        assertThat(userRole.getRoleNames(scriptService, model), contains("_user_hulk"));

        final TemplateRole groupsRole = new TemplateRole(new BytesArray("{ \"source\":\"{{#tojson}}groups{{/tojson}}\" }"),
            TemplateRole.Format.JSON);
        assertThat(groupsRole.getRoleNames(scriptService, model), contains("avengers", "defenders", "panthenon"));
    }

    private MappedRole parse(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        final MappedRole role = MappedRole.parse(parser);
        assertThat(role, notNullValue());
        return role;
    }

    public void trySerialize(MappedRole original) throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeNamedWriteable(original);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        final StreamInput rawInput = ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes()));
        final StreamInput streamInput = new NamedWriteableAwareStreamInput(rawInput, registry);
        final MappedRole serialized = streamInput.readNamedWriteable(MappedRole.class);
        assertEquals(original, serialized);
    }

    public void tryEquals(MappedRole original) {
        final EqualsHashCodeTestUtils.CopyFunction<MappedRole> copy = mr -> {
            if (mr instanceof NamedRole) {
                return new NamedRole(((NamedRole) mr).getRoleName());
            } else {
                TemplateRole tr = (TemplateRole) mr;
                return new TemplateRole(tr.getTemplate(), tr.getFormat());
            }
        };
        final EqualsHashCodeTestUtils.MutateFunction<MappedRole> mutate = mr -> {
            if (mr instanceof NamedRole) {
                return new NamedRole(randomAlphaOfLength(1) + ((NamedRole) mr).getRoleName());
            } else {
                TemplateRole tr = (TemplateRole) mr;
                if (randomBoolean()) {
                    return new TemplateRole(tr.getTemplate(),
                        randomValueOtherThan(tr.getFormat(), () -> randomFrom(TemplateRole.Format.values())));
                } else {
                    final String templateStr = tr.getTemplate().utf8ToString();
                    return new TemplateRole(new BytesArray(templateStr.substring(randomIntBetween(1, templateStr.length() / 2))),
                        tr.getFormat());
                }
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, copy, mutate);
    }

}
