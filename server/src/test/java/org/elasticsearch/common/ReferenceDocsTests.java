/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;

import static org.elasticsearch.common.ReferenceDocs.getVersionComponent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class ReferenceDocsTests extends ESTestCase {

    public void testVersionComponent() {
        // Snapshot x.y.0 versions are unreleased so link to master
        assertEquals("master", getVersionComponent("8.11.0", true));

        // Snapshot x.y.z versions with z>0 mean that x.y.0 is released so have a properly versioned docs link
        assertEquals("8.5", getVersionComponent("8.5.1", true));

        // Non-snapshot versions are to be released so have a properly versioned docs link
        assertEquals("8.7", getVersionComponent("8.7.0", false));

        // Non-snapshot non-semantic versions link to latest docs
        assertEquals("current", getVersionComponent("ABCDEF", false));

        // Snapshot non-semantic versions are considered unreleased so link to master
        assertEquals("master", getVersionComponent("ABCDEF", true));
    }

    private static final String TEST_LINK_PLACEHOLDER = "TEST_LINK";

    private interface LinkSupplier {
        String mutateLinkLine(int index, String lineWithPlaceholder);
    }

    private static InputStream getResourceStream(LinkSupplier linkSupplier) {
        final var stringBuilder = new StringBuilder();
        for (int i = 0; i < ReferenceDocs.values().length; i++) {
            final var symbol = ReferenceDocs.values()[i];
            final var lineWithPlaceholder = symbol.name() + " ".repeat(ReferenceDocs.SYMBOL_COLUMN_WIDTH - symbol.name().length())
                + TEST_LINK_PLACEHOLDER;
            final var updatedLine = linkSupplier.mutateLinkLine(i, lineWithPlaceholder);
            if (updatedLine == null) {
                break;
            } else {
                stringBuilder.append(updatedLine).append('\n');
            }
        }
        return new BytesArray(stringBuilder.toString()).streamInput();
    }

    public void testSuccessNoFragments() throws IOException {
        final var linksMap = ReferenceDocs.readLinksBySymbol(getResourceStream((i, l) -> l));
        assertEquals(ReferenceDocs.values().length, linksMap.size());
        for (ReferenceDocs link : ReferenceDocs.values()) {
            assertEquals(new ReferenceDocs.LinkComponents(TEST_LINK_PLACEHOLDER, ""), linksMap.get(link.name()));
        }
    }

    public void testSuccessWithFragments() throws IOException {
        final var linksMap = ReferenceDocs.readLinksBySymbol(getResourceStream((i, l) -> l + "#test-fragment"));
        assertEquals(ReferenceDocs.values().length, linksMap.size());
        for (ReferenceDocs link : ReferenceDocs.values()) {
            assertEquals(new ReferenceDocs.LinkComponents(TEST_LINK_PLACEHOLDER, "#test-fragment"), linksMap.get(link.name()));
        }
    }

    public void testTruncated() {
        final var targetLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(getResourceStream((i, l) -> i == targetLine ? null : l))
            ).getMessage(),
            equalTo("links resource truncated at line " + (targetLine + 1))
        );
    }

    public void testMissingLink() {
        final var targetLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(
                    getResourceStream((i, l) -> i == targetLine ? l.replace(TEST_LINK_PLACEHOLDER, "") : l)
                )
            ).getMessage(),
            equalTo("no link found for [" + ReferenceDocs.values()[targetLine].name() + "] at line " + (targetLine + 1))
        );
    }

    public void testUnexpectedSymbol() {
        final var targetSymbol = randomFrom(ReferenceDocs.values()).name();
        final var replacement = "x".repeat(targetSymbol.length());
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(getResourceStream((i, l) -> l.replace(targetSymbol, replacement)))
            ).getMessage(),
            startsWith("unexpected symbol at line ")
        );
    }

    public void testWhitespace() {
        final var leadingWhitespaceLine = between(0, ReferenceDocs.values().length - 1);
        final var trailingWhitespaceLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(
                    getResourceStream(
                        (i, l) -> l.replace(
                            TEST_LINK_PLACEHOLDER,
                            (i == leadingWhitespaceLine ? "  " : "") + TEST_LINK_PLACEHOLDER + (i == trailingWhitespaceLine ? "  " : "")
                        )
                    )
                )
            ).getMessage(),
            startsWith("unexpected content at line " + (Math.min(leadingWhitespaceLine, trailingWhitespaceLine) + 1) + ": expected [")
        );
    }

    public void testTrailingContent() throws IOException {
        final byte[] validContent;
        try (var stream = getResourceStream((i, l) -> l)) {
            validContent = stream.readAllBytes();
        }
        final BytesReference contentWithTrailingData = CompositeBytesReference.of(new BytesArray(validContent), new BytesArray("x"));

        assertThat(
            expectThrows(IllegalStateException.class, () -> ReferenceDocs.readLinksBySymbol(contentWithTrailingData.streamInput()))
                .getMessage(),
            equalTo("unexpected trailing content at line " + (ReferenceDocs.values().length + 1))
        );
    }

    public void testRejectsAutoGeneratedFragment() {
        final var targetLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(
                    getResourceStream(
                        (i, l) -> i == targetLine ? l.replace(TEST_LINK_PLACEHOLDER, "test.html#_auto_generated_fragment") : l
                    )
                )
            ).getMessage(),
            equalTo(
                "found auto-generated fragment ID in link [test.html#_auto_generated_fragment] for ["
                    + ReferenceDocs.values()[targetLine].name()
                    + "] at line "
                    + (targetLine + 1)
            )
        );
    }

    public void testRejectsAutoGeneratedPageName() {
        final var targetLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(
                    getResourceStream((i, l) -> i == targetLine ? l.replace(TEST_LINK_PLACEHOLDER, "_auto_generated_page.html") : l)
                )
            ).getMessage(),
            equalTo(
                "found auto-generated fragment ID in link [_auto_generated_page.html] for ["
                    + ReferenceDocs.values()[targetLine].name()
                    + "] at line "
                    + (targetLine + 1)
            )
        );
    }

    public void testRejectsQueryPart() {
        final var targetLine = between(0, ReferenceDocs.values().length - 1);
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> ReferenceDocs.readLinksBySymbol(
                    getResourceStream((i, l) -> i == targetLine ? l.replace(TEST_LINK_PLACEHOLDER, "foo/bar?baz=quux") : l)
                )
            ).getMessage(),
            equalTo("ReferenceDocs does not support links containing pre-existing query parameters: foo/bar?baz=quux")
        );
    }

    // for manual verification
    public void testShowAllLinks() {
        for (final var link : ReferenceDocs.values()) {
            logger.info("--> {}", link);
        }
    }

    // for manual verification that the rendered links appear to be reasonable
    public void testToStringLooksOk() {
        // test one without a fragment ID
        assertEquals(
            "https://www.elastic.co/docs/troubleshoot/elasticsearch/discovery-troubleshooting?version=" + ReferenceDocs.VERSION_COMPONENT,
            ReferenceDocs.DISCOVERY_TROUBLESHOOTING.toString()
        );
        // test another that has a fragment ID
        assertEquals(
            "https://www.elastic.co/docs/deploy-manage/deploy/self-managed/important-settings-configuration?version="
                + ReferenceDocs.VERSION_COMPONENT
                + "#initial_master_nodes",
            ReferenceDocs.INITIAL_MASTER_NODES.toString()
        );
    }
}
