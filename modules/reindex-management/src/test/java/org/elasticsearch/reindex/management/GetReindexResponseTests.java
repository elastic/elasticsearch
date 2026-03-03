/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.elasticsearch.reindex.management.GetReindexResponse.sanitizeDescription;
import static org.hamcrest.Matchers.equalTo;

public class GetReindexResponseTests extends ESTestCase {

    public void testSanitizeDescriptionNull() {
        assertThat(sanitizeDescription(null), equalTo(Optional.empty()));
    }

    public void testSanitizeDescriptionLocalReindex() {
        assertThat(sanitizeDescription("reindex from [source] to [dest]"), equalTo(Optional.of("reindex from [source] to [dest]")));
    }

    public void testSanitizeDescriptionLocalReindexMultipleIndices() {
        assertThat(
            sanitizeDescription("reindex from [source1, source2] to [dest]"),
            equalTo(Optional.of("reindex from [source1, source2] to [dest]"))
        );
    }

    public void testSanitizeDescriptionLocalReindexWithScript() {
        assertThat(
            sanitizeDescription(
                "reindex from [source] updated with Script{type=inline, lang='painless',"
                    + " idOrCode='ctx._source.tag = 'host=localhost port=9200 username=admin password=secret'',"
                    + " options={}, params={}}"
                    + " to [dest]"
            ),
            equalTo(Optional.of("reindex from [source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionNonReindexDescription() {
        assertThat(sanitizeDescription("some other task description"), equalTo(Optional.empty()));
    }

    public void testSanitizeDescriptionRemoteWithAllFields() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}} username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionRemoteQueryOnly() {
        assertThat(
            sanitizeDescription("reindex from [host=example.com port=9200 query={\"match_all\":{}}][source] to [dest]"),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionRemoteUsernameOnly() {
        assertThat(
            sanitizeDescription("reindex from [host=example.com port=9200 query={\"match_all\":{}} username=real_user][source] to [dest]"),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionRemoteWithSchemeAndPathPrefix() {
        assertThat(
            sanitizeDescription(
                "reindex from [scheme=https host=example.com port=9200 pathPrefix=/es query={\"match_all\":{}}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [scheme=https host=example.com port=9200 pathPrefix=/es][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryWithPrettyPrintedJson() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\n  \"match_all\" : {\n    \"boost\" : 1.0\n  }\n}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryWithArrayBrackets() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"terms\":{\"status\":[\"active\",\"pending\"]}}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryWithNestedArrayBrackets() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"bool\":{\"should\":[{\"terms\":{\"x\":[\"a\",\"b\"]}},"
                    + "{\"terms\":{\"y\":[\"c\"]}}]}} username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryWithLuceneRangeSyntax() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"query_string\":{\"query\":\"field:[1 TO 10]\"}}][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryContainingUsernameEquals() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"query_string\":{\"query\":\" username=admin\"}}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryContainingPasswordEquals() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"query_string\":{\"query\":\" password=secret\"}}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryContainingUsernameFieldName() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"term\":{\"username\":\"john\"}}"
                    + " username=real_user password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionQueryContainingBracketPair() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"query_string\":{\"query\":\"a][b\"}}"
                    + " username=real_user][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionUsernameWithBrackets() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}}" + " username=user]name password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionUsernameWithBracketPair() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}} username=user][name password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionUsernameWithSpecialChars() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}} username=user@domain[0] password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionUsernameWithWhitespace() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}} username=user name password=<<>>][source] to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }

    public void testSanitizeDescriptionRemoteWithScript() {
        assertThat(
            sanitizeDescription(
                "reindex from [host=example.com port=9200 query={\"match_all\":{}} username=real_user password=<<>>][source]"
                    + " updated with Script{type=inline, lang='painless',"
                    + " idOrCode='ctx._source.tag = 'host=localhost port=9200 username=admin password=secret'',"
                    + " options={}, params={}}"
                    + " to [dest]"
            ),
            equalTo(Optional.of("reindex from [host=example.com port=9200][source] to [dest]"))
        );
    }
}
