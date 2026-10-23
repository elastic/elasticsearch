/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.greaterThan;

public class UnmappedFieldsPatternTests extends AbstractNamedWriteableTestCase<UnmappedFieldsPattern> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(UnmappedFieldsPattern.ENTRY));
    }

    @Override
    protected Class<UnmappedFieldsPattern> categoryClass() {
        return UnmappedFieldsPattern.class;
    }

    @Override
    protected UnmappedFieldsPattern createTestInstance() {
        return switch (between(0, 4)) {
            case 0 -> UnmappedFieldsPattern.ALL;
            case 1 -> UnmappedFieldsPattern.NONE;
            case 2 -> UnmappedFieldsPattern.includes(List.of("first*", "given*"))
                .intersect(UnmappedFieldsPattern.includes(List.of("last*", "family*")))
                .withAdditionalExcludes(List.of("secret*", "emp_no"));
            case 3 -> UnmappedFieldsPattern.excludes(List.of(randomAlphaOfLength(4) + "*"));
            case 4 -> randomPattern();
            default -> throw new AssertionError("unreachable");
        };
    }

    @Override
    protected UnmappedFieldsPattern mutateInstance(UnmappedFieldsPattern instance) {
        if (instance.isNone()) {
            return UnmappedFieldsPattern.ALL;
        }
        if (instance.equals(UnmappedFieldsPattern.ALL)) {
            return UnmappedFieldsPattern.NONE;
        }
        return randomBoolean()
            ? instance.intersect(UnmappedFieldsPattern.includes(List.of("mutation_" + randomAlphaOfLength(4) + "*")))
            : instance.withAdditionalExcludes(List.of("mutation_" + randomAlphaOfLength(4)));
    }

    public void testObjectPushPrunesOnlyOnSubtreeCoveringExcludes() {
        // Exact excludes - what DetermineUnmappedFieldsToKeep adds for a referenced/dropped/mapped column - never prune an object.
        UnmappedFieldsPattern exact = UnmappedFieldsPattern.excludes(List.of("unmapped", "id"));
        assertTrue(exact.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(exact.objectSubfieldsCouldMatch("id"));

        // A prefix wildcard ending in * covers the whole subtree, so it prunes the object at the data node.
        UnmappedFieldsPattern prefix = UnmappedFieldsPattern.excludes(List.of("unmapped*"));
        assertFalse(prefix.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(prefix.objectSubfieldsCouldMatch("other"));

        // A fixed-suffix or interior wildcard matches the parent name but not its deeper leaves, so it must NOT prune the object here
        // (regression guard: this used to prune and silently lose synthetic-source leaves that a stored source would have kept).
        assertTrue(UnmappedFieldsPattern.excludes(List.of("*ped")).objectSubfieldsCouldMatch("unmapped"));
        assertTrue(UnmappedFieldsPattern.excludes(List.of("un*ped")).objectSubfieldsCouldMatch("unmapped"));

        // A nested-wildcard drop targets a descendant subtree, not the parent, so the parent object still ships.
        UnmappedFieldsPattern nested = UnmappedFieldsPattern.excludes(List.of("unmapped.deep*"));
        assertTrue(nested.objectSubfieldsCouldMatch("unmapped"));

        assertFalse(UnmappedFieldsPattern.NONE.objectSubfieldsCouldMatch("unmapped"));
        assertTrue(UnmappedFieldsPattern.ALL.objectSubfieldsCouldMatch("unmapped"));
    }

    public void testObjectPushShipsOnlyObjectsAnIncludeGroupCanReach() {
        // `KEEP network*` where "network" is also a mapped column: DetermineUnmappedFieldsToKeep adds the mapped name as an exact
        // exclude, and the wildcard leg still reaches network.<leaf>, so the object ships. An unreachable object is pruned instead.
        UnmappedFieldsPattern keepWildcardExact = UnmappedFieldsPattern.includes(List.of("network*"))
            .withAdditionalExcludes(List.of("network"));
        assertTrue(keepWildcardExact.objectSubfieldsCouldMatch("network"));
        assertFalse(keepWildcardExact.objectSubfieldsCouldMatch("unrelated"));

        UnmappedFieldsPattern keepWildcard = UnmappedFieldsPattern.includes(List.of("keep*"));
        assertTrue(keepWildcard.objectSubfieldsCouldMatch("keep_me"));
        assertFalse(keepWildcard.objectSubfieldsCouldMatch("other"));

        // A dotted include reaches the object whose subtree it targets, and nothing else.
        UnmappedFieldsPattern dotted = UnmappedFieldsPattern.includes(List.of("network.eth0.*"));
        assertTrue(dotted.objectSubfieldsCouldMatch("network"));
        assertFalse(dotted.objectSubfieldsCouldMatch("system"));

        // Every include group must independently be able to reach the object (chained KEEPs intersect); one that cannot prunes it.
        UnmappedFieldsPattern intersect = UnmappedFieldsPattern.includes(List.of("network.*"))
            .intersect(UnmappedFieldsPattern.includes(List.of("system.*")));
        assertFalse(intersect.objectSubfieldsCouldMatch("network"));
        assertFalse(intersect.objectSubfieldsCouldMatch("system"));
    }

    public void testObjectPushNeverUnderShipsAReachableDescendant() {
        assertTrue(UnmappedFieldsPattern.includes(List.of("a.*.leaf")).objectSubfieldsCouldMatch("a.b"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("*.leaf")).objectSubfieldsCouldMatch("a"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("a*")).objectSubfieldsCouldMatch("abc"));
        assertTrue(UnmappedFieldsPattern.includes(List.of("unmapped.deep.leaf")).objectSubfieldsCouldMatch("unmapped"));
        assertFalse(UnmappedFieldsPattern.includes(List.of("ab")).objectSubfieldsCouldMatch("a"));
        assertFalse(UnmappedFieldsPattern.includes(List.of("nx.y")).objectSubfieldsCouldMatch("n"));
    }

    public void testMatchesGovernsDottedLeavesForSourceParity() {
        UnmappedFieldsPattern exactExclude = UnmappedFieldsPattern.excludes(List.of("unmapped"));
        assertFalse(exactExclude.matches("unmapped"));
        assertTrue(exactExclude.matches("unmapped.deep.leaf"));

        UnmappedFieldsPattern childWildcardKeep = UnmappedFieldsPattern.includes(List.of("unmapped.*"));
        assertTrue(childWildcardKeep.matches("unmapped.deep.leaf"));
        assertTrue(childWildcardKeep.matches("unmapped.foo"));
        assertFalse(childWildcardKeep.matches("unmapped"));
        assertFalse(childWildcardKeep.matches("other.leaf"));

        UnmappedFieldsPattern nestedWildcardDrop = UnmappedFieldsPattern.excludes(List.of("unmapped.deep*"));
        assertFalse(nestedWildcardDrop.matches("unmapped.deep.leaf"));
        assertTrue(nestedWildcardDrop.matches("unmapped.foo"));

        UnmappedFieldsPattern exactInclude = UnmappedFieldsPattern.includes(List.of("network"));
        assertTrue(exactInclude.matches("network"));
        assertFalse(exactInclude.matches("network.bytes_in"));

        UnmappedFieldsPattern fixedSuffixDrop = UnmappedFieldsPattern.excludes(List.of("*ped"));
        assertFalse(fixedSuffixDrop.matches("unmapped"));
        assertTrue(fixedSuffixDrop.matches("unmapped.deep.leaf"));
    }

    public void testExactExcludeWithLiteralStarIsMatchedLiterallyNotAsWildcard() {
        // this is about a backtick-escaped KEEP term like `samples*` that refers to a column named literally "samples*"
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.ALL.withAdditionalExcludes(List.of("samples*"));
        assertFalse(pattern.matches("samples*"));
        assertTrue(pattern.matches("samples.nested"));
        assertTrue(pattern.objectSubfieldsCouldMatch("samples"));
    }

    public void testUnionOfRestrictiveKeepAndAllKeepsTheAllSide() {
        UnmappedFieldsPattern keepMessage = UnmappedFieldsPattern.includes(List.of("messag*")).withAdditionalExcludes(List.of("message"));
        UnmappedFieldsPattern whereAll = UnmappedFieldsPattern.ALL.withAdditionalExcludes(List.of("message", "@timestamp"));
        UnmappedFieldsPattern union = keepMessage.union(whereAll);
        assertTrue(union.matches("language_code"));
        assertTrue(union.matches("unmapped.nested"));
        assertFalse(union.matches("message"));
    }

    public void testUnionOfTwoKeepWildcardsIsOr() {
        UnmappedFieldsPattern union = UnmappedFieldsPattern.includes(List.of("first*"))
            .union(UnmappedFieldsPattern.includes(List.of("last*")));
        assertTrue(union.matches("first_name"));
        assertTrue(union.matches("last_name"));
        assertFalse(union.matches("salary"));
    }

    public void testForKeepExactNamesDontReachIncludeGroups() {
        var exactOnly = UnmappedFieldsPattern.forKeep(List.of(new UnresolvedAttribute(Source.EMPTY, "foo")));
        assertTrue(exactOnly.isNone());
        assertFalse(exactOnly.objectSubfieldsCouldMatch("foo"));

        var mixed = UnmappedFieldsPattern.forKeep(
            List.of(new UnresolvedAttribute(Source.EMPTY, "foo"), new UnresolvedNamePattern(Source.EMPTY, null, "bar*", "bar*", "bar*"))
        );
        assertFalse(mixed.objectSubfieldsCouldMatch("foo"));
        assertTrue(mixed.objectSubfieldsCouldMatch("bar"));
    }

    public void testForKeepEscapedStarIsLiteral() {
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.forKeep(List.of(namePattern("a\\*b*")));
        assertTrue(pattern.matches("a*b"));
        assertTrue(pattern.matches("a*b.c"));
        assertFalse(pattern.matches("ab"));
        assertFalse(pattern.matches("axb"));
        assertTrue(pattern.objectSubfieldsCouldMatch("a*b"));
        assertFalse(pattern.objectSubfieldsCouldMatch("a"));
        assertFalse(pattern.objectSubfieldsCouldMatch("axb"));
    }

    public void testForKeepInteriorEscapedStars() {
        UnmappedFieldsPattern interior = UnmappedFieldsPattern.forKeep(List.of(namePattern("*x\\*y*z\\*")));
        assertTrue(interior.matches("x*yz*"));
        assertTrue(interior.matches("ax*ybz*"));
        assertFalse(interior.matches("xayz*"));
        assertFalse(interior.matches("x*yz"));
        assertFalse(interior.matches("x*y*"));
    }

    public void testForKeepEscapedBackslashIsLiteral() {
        UnmappedFieldsPattern pattern = UnmappedFieldsPattern.forKeep(List.of(namePattern("a\\\\*")));
        assertTrue(pattern.matches("a\\"));
        assertTrue(pattern.matches("a\\b"));
        assertFalse(pattern.matches("a"));
        assertFalse(pattern.matches("ab"));
        assertFalse(pattern.matches("a*"));
    }

    public void testForDropPrunesOnlyOnAnUnescapedTrailingWildcard() {
        UnmappedFieldsPattern escapedStarSuffix = UnmappedFieldsPattern.forDrop(List.of(namePattern("*d\\*")));
        assertFalse(escapedStarSuffix.matches("unmapped*"));
        assertTrue(escapedStarSuffix.matches("unmapped"));
        assertTrue(escapedStarSuffix.objectSubfieldsCouldMatch("unmappedd*"));
    }

    public void testForDropEscapedBackslashBeforeWildcard() {
        UnmappedFieldsPattern escapedBackslashThenWildcard = UnmappedFieldsPattern.forDrop(List.of(namePattern("a\\\\*")));
        assertFalse(escapedBackslashThenWildcard.matches("a\\b"));
        assertFalse(escapedBackslashThenWildcard.objectSubfieldsCouldMatch("a\\"));
        assertTrue(escapedBackslashThenWildcard.objectSubfieldsCouldMatch("ab"));
    }

    public void testMatchesAgreesWithSimpleMatchOnEscapeFreeGlobs() {
        for (int i = 0; i < 500; i++) {
            String include = randomGlob(false);
            String alternative = randomGlob(false);
            String otherGroup = randomGlob(false);
            String exclude = randomGlob(false);
            UnmappedFieldsPattern pattern = UnmappedFieldsPattern.includes(List.of(include, alternative))
                .intersect(UnmappedFieldsPattern.includes(List.of(otherGroup)))
                .intersect(UnmappedFieldsPattern.excludes(List.of(exclude)));
            for (int j = 0; j < 20; j++) {
                String name = randomName();
                String message = pattern + " against [" + name + "]";
                if (Regex.simpleMatch(exclude, name)) {
                    assertFalse(message, pattern.matches(name));
                } else {
                    boolean matchesInclude = Regex.simpleMatch(include, name) || Regex.simpleMatch(alternative, name);
                    boolean matchesOtherGroup = Regex.simpleMatch(otherGroup, name);
                    assertEquals(message, matchesInclude && matchesOtherGroup, pattern.matches(name));
                }
            }
        }
    }

    public void testMatchesAgreesWithARegexOnEscapedGlobs() {
        for (int i = 0; i < 500; i++) {
            List<String> tokens = randomList(0, 5, () -> randomFrom("a", "b", ".", "*", "\\*", "\\\\"));
            StringBuilder regex = new StringBuilder();
            for (String token : tokens) {
                regex.append(token.equals("*") ? ".*" : Pattern.quote(token.length() == 2 ? token.substring(1) : token));
            }
            Pattern reference = Pattern.compile(regex.toString(), Pattern.DOTALL);
            UnmappedFieldsPattern pattern = UnmappedFieldsPattern.includes(List.of(String.join("", tokens)));
            for (int j = 0; j < 20; j++) {
                String name = randomName();
                assertEquals(pattern + " against [" + name + "]", reference.matcher(name).matches(), pattern.matches(name));
            }
        }
    }

    public void testObjectSubfieldsCouldMatchWheneverTheKeyOrADescendantMatches() {
        int matched = 0;
        for (int i = 0; i < 500; i++) {
            UnmappedFieldsPattern pattern = randomPattern();
            for (int j = 0; j < 20; j++) {
                String name = randomName();
                if (pattern.matches(name)) {
                    matched++;
                    assertTrue(pattern + " must ship [" + name + "]", pattern.objectSubfieldsCouldMatch(name));
                    for (int dot = name.indexOf('.'); dot >= 0; dot = name.indexOf('.', dot + 1)) {
                        String key = name.substring(0, dot);
                        assertTrue(pattern + " must ship [" + key + "] for [" + name + "]", pattern.objectSubfieldsCouldMatch(key));
                    }
                }
            }
        }
        assertThat(matched, greaterThan(0));
    }

    public void testUnionKeepsAndShipsWhateverEitherSideDoes() {
        for (int i = 0; i < 500; i++) {
            UnmappedFieldsPattern left = randomPattern();
            UnmappedFieldsPattern right = randomPattern();
            UnmappedFieldsPattern union = left.union(right);
            for (int j = 0; j < 20; j++) {
                String name = randomName();
                if (left.matches(name) || right.matches(name)) {
                    assertTrue(union + " must keep [" + name + "]", union.matches(name));
                }
                if (left.objectSubfieldsCouldMatch(name) || right.objectSubfieldsCouldMatch(name)) {
                    assertTrue(union + " must ship [" + name + "]", union.objectSubfieldsCouldMatch(name));
                }
            }
        }
    }

    public void testDeserializedCopyMatchesTheSameNames() throws IOException {
        for (int i = 0; i < 50; i++) {
            UnmappedFieldsPattern pattern = randomPattern();
            UnmappedFieldsPattern copy = copyInstance(pattern, TransportVersion.current());
            for (int j = 0; j < 20; j++) {
                String name = randomName();
                assertEquals(pattern + " against [" + name + "]", pattern.matches(name), copy.matches(name));
                assertEquals(
                    pattern + " against [" + name + "]",
                    pattern.objectSubfieldsCouldMatch(name),
                    copy.objectSubfieldsCouldMatch(name)
                );
            }
        }
    }

    private static UnmappedFieldsPattern randomPattern() {
        UnmappedFieldsPattern includes = randomBoolean()
            ? UnmappedFieldsPattern.ALL
            : UnmappedFieldsPattern.includes(List.of(randomGlob(true), randomGlob(true)))
                .intersect(UnmappedFieldsPattern.includes(List.of(randomGlob(true))));
        return includes.intersect(UnmappedFieldsPattern.excludes(List.of(randomGlob(true)))).withAdditionalExcludes(List.of(randomName()));
    }

    private static String randomGlob(boolean withEscapes) {
        List<String> tokens = withEscapes ? List.of("a", "b", ".", "*", "\\*", "\\\\") : List.of("a", "b", ".", "*");
        return String.join("", randomList(0, 5, () -> randomFrom(tokens)));
    }

    private static String randomName() {
        return String.join("", randomList(0, 7, () -> randomFrom("a", "b", ".", "*", "\\")));
    }

    private static UnresolvedNamePattern namePattern(String glob) {
        return new UnresolvedNamePattern(Source.EMPTY, null, glob, glob, glob);
    }
}
