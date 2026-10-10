/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class DlsLookupTests extends ESTestCase {

    public void testNonTemplateQueryDeclaresNoLookups() {
        assertThat(DlsLookup.extractFromRoleQuery(new BytesArray("{\"term\":{\"clearance\":\"public\"}}")), is(empty()));
    }

    public void testTemplateWithoutLookupsDeclaresNone() {
        assertThat(DlsLookup.extractFromRoleQuery(new BytesArray("{\"template\":{\"source\":\"{}\"}}")), is(empty()));
    }

    public void testExtractLookups() {
        final List<DlsLookup> lookups = DlsLookup.extractFromRoleQuery(new BytesArray("""
            {
              "template": { "source": "{\\"terms\\":{\\"ml_job_id\\":{{#toJson}}_lookup.ml_jobs{{/toJson}}}}" },
              "lookups": {
                "ml_jobs": { "type": "ml_job_ids", "params": { "spaces": ["marketing", "sales"] } },
                "owner": { "type": "profile_uid" }
              }
            }"""));
        assertThat(
            lookups,
            contains(
                new DlsLookup("ml_jobs", "ml_job_ids", Map.of("spaces", List.of("marketing", "sales"))),
                new DlsLookup("owner", "profile_uid", Map.of())
            )
        );
    }

    public void testSiblingsOfTemplateOtherThanLookupsAreSkipped() {
        // Trailing fields were never inspected before lookups existed; keep tolerating them so existing roles keep working.
        final List<DlsLookup> lookups = DlsLookup.extractFromRoleQuery(new BytesArray("""
            {
              "template": { "source": "{}" },
              "unrelated": { "nested": [1, 2, { "deep": true }] },
              "lookups": { "a": { "type": "t" } },
              "another": "value"
            }"""));
        assertThat(lookups, contains(new DlsLookup("a", "t", Map.of())));
    }

    public void testKeyIgnoresNameAndParamOrder() {
        final Map<String, Object> params1 = Map.of("spaces", List.of("a", "b"), "nested", Map.of("x", 1, "y", 2));
        final Map<String, Object> params2 = Map.of("nested", Map.of("y", 2, "x", 1), "spaces", List.of("a", "b"));
        final DlsLookup lookup1 = new DlsLookup("first", "type", params1);
        final DlsLookup lookup2 = new DlsLookup("second", "type", params2);
        assertThat(lookup1.key(), equalTo(lookup2.key()));
        assertThat(lookup1.key(), equalTo("""
            {"type":"type","params":{"nested":{"x":1,"y":2},"spaces":["a","b"]}}"""));

        assertThat(new DlsLookup("first", "other_type", params1).key(), not(equalTo(lookup1.key())));
        assertThat(new DlsLookup("first", "type", Map.of("spaces", List.of("b", "a"))).key(), not(equalTo(lookup1.key())));
        assertThat(new DlsLookup("first", "type", Map.of()).key(), equalTo("""
            {"type":"type","params":{}}"""));
    }

    public void testInvalidNameIsRejected() {
        final String badName = randomFrom("with-dash", "with.dot", "with space", "", "{{mustache}}");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new DlsLookup(badName, "t", Map.of()));
        assertThat(e.getMessage(), containsString("invalid DLS lookup name"));

        final ElasticsearchParseException parseException = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(
                new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":{\"" + badName + "\":{\"type\":\"t\"}}}")
            )
        );
        assertThat(parseException.getMessage(), containsString("invalid DLS lookup name"));
    }

    public void testMissingTypeIsRejected() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(
                new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"params\":{\"x\":1}}}}")
            )
        );
        assertThat(e.getMessage(), equalTo("lookup [a] is missing required field [type]"));
    }

    public void testEmptyTypeIsRejected() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"type\":\" \"}}}"))
        );
        assertThat(e.getMessage(), equalTo("DLS lookup [a] must declare a non-empty type"));
    }

    public void testUnknownLookupFieldIsRejected() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(
                new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"type\":\"t\",\"context\":{}}}}")
            )
        );
        assertThat(e.getMessage(), equalTo("unknown field [context] in lookup [a]"));
    }

    public void testNonObjectParamsAreRejected() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(
                new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"type\":\"t\",\"params\":[1]}}}")
            )
        );
        assertThat(e.getMessage(), containsString("expected [params] of lookup [a] to be an object"));
    }

    public void testDuplicateFieldsAreRejectedByTheParser() {
        // Elasticsearch's JSON parser refuses duplicate keys, so neither a repeated lookup name nor a second [lookups] object
        // can be smuggled in to override an earlier declaration.
        final String duplicateName = "{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"type\":\"t\"},\"a\":{\"type\":\"u\"}}}";
        final String duplicateLookups =
            "{\"template\":{\"source\":\"{}\"},\"lookups\":{\"a\":{\"type\":\"t\"}},\"lookups\":{\"b\":{\"type\":\"t\"}}}";
        final XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> DlsLookup.extractFromRoleQuery(new BytesArray(randomFrom(duplicateName, duplicateLookups)))
        );
        assertThat(e.getMessage(), containsString("Duplicate field"));
    }

    public void testLookupsMustBeAnObject() {
        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> DlsLookup.extractFromRoleQuery(new BytesArray("{\"template\":{\"source\":\"{}\"},\"lookups\":[]}"))
        );
        assertThat(e.getMessage(), containsString("expected [lookups] to be an object"));
    }
}
