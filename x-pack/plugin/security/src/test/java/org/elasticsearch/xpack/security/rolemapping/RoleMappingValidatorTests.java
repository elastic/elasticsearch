/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rolemapping;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.hamcrest.Matchers;

import java.io.IOException;

public class RoleMappingValidatorTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder().put("xpack.security.role_mapping.rules.strict", "true").build();

    public void testSimpleFieldExpressionWithoutRealm() throws IOException {
        final String json = """
            { "field": { "username" : "*@shield.gov" } }
            """;
        RoleMapperExpression expression = parse(json);
        var e = expectThrows(Exception.class, () -> RoleMappingValidator.validateMappingRules(expression, SETTINGS));
        assertThat(e.getMessage(), Matchers.containsString("role mapping must be scoped to a concrete realm name"));
    }

    public void testComplexExpressionWithoutRealm() throws IOException {
        final String json = """
            { "any": [
              { "field": { "username" : "*@shield.gov" } },
              { "all": [
                { "field": { "username" : "/.*\\\\@avengers\\\\.(net|org)/" } },
                { "field": { "groups" : [ "admin", "operators" ] } },
                { "except":
                  { "field": { "groups" : "disavowed" } }
                }
              ] }
            ] }""";
        final RoleMapperExpression expression = parse(json);

        var e = expectThrows(Exception.class, () -> RoleMappingValidator.validateMappingRules(expression, SETTINGS));
        assertThat(e.getMessage(), Matchers.containsString("role mapping must be scoped to a concrete realm name"));
    }

    public void testComplexExpressionWithoutRealmInAnyExpressions() throws IOException {
        final String json = """
            { "any": [
              { "field": { "username" : "*@shield.gov" } },
              { "all": [
                { "field": { "username" : "/.*\\\\@avengers\\\\.(net|org)/" } },
                { "field": { "realm.name" : "saml1" } },
                { "field": { "groups" : [ "admin", "operators" ] } },
                { "except":
                  { "field": { "groups" : "disavowed" } }
                }
              ] }
            ] }""";
        final RoleMapperExpression expression = parse(json);

        var e = expectThrows(Exception.class, () -> RoleMappingValidator.validateMappingRules(expression, SETTINGS));
        assertThat(e.getMessage(), Matchers.containsString("role mapping must be scoped to a concrete realm name"));
    }

    public void testSimpleExpressionWithUnallowedWildcardRealmRestriction() throws IOException {
        final String json = """
            { "field": { "realm.name": "*" } }
            """;
        final RoleMapperExpression expression = parse(json);
        var e = expectThrows(Exception.class, () -> RoleMappingValidator.validateMappingRules(expression, SETTINGS));
        assertThat(e.getMessage(), Matchers.containsString("role mapping must be scoped to a concrete realm name"));
    }

    public void testSimpleExpressionWithRealmRestriction() throws IOException {
        final String json = """
            { "field": { "realm.name": [ "jwt1", "jwt2" ] } }
            """;
        RoleMapperExpression expression = parse(json);
        RoleMappingValidator.validateMappingRules(expression, SETTINGS);
    }

    public void testComplexAnyExpressionWithRealm() throws IOException {
        final String json = """
            { "any": [
              { "all": [
                { "field": { "username" : "*@shield.gov" } },
                { "field": { "realm.name" : "jwt1" } }
              ] },
              { "all": [
                { "field": { "username" : "*" } },
                { "field": { "realm.name" : "jwt2" } },
                { "except":
                  { "field": { "realm.name" : "ignored" } }
                }
              ] }
            ] }""";
        final RoleMapperExpression expression = parse(json);
        RoleMappingValidator.validateMappingRules(expression, SETTINGS);
    }

    public void testComplexAllExpressionWithRealm() throws IOException {
        final String json = """
            { "all": [
              { "any": [
                { "field": { "realm.name" : "jwt1" } },
                { "field": { "realm.name" : "jwt2" } }
              ] },
              { "field": { "username" : "*" } }
            ] }""";
        final RoleMapperExpression expression = parse(json);
        RoleMappingValidator.validateMappingRules(expression, SETTINGS);
    }

    private RoleMapperExpression parse(String json) throws IOException {
        return new ExpressionParser().parse("rules", new XContentSource(new BytesArray(json), XContentType.JSON));
    }
}
