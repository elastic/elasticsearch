/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.common.ReferenceDocs.getVersionComponent;

public class ReferenceDocsTests extends ESTestCase {

    public void testVersionComponent() {
        // Snapshot x.y.0 versions are unreleased so link to master
        assertEquals("master", getVersionComponent(Version.V_8_7_0, true));

        // Snapshot x.y.z versions with z>0 mean that x.y.0 is released so have a properly versioned docs link
        assertEquals("8.5", getVersionComponent(Version.V_8_5_1, true));

        // Non-snapshot versions are to be released so have a properly versioned docs link
        assertEquals("8.7", getVersionComponent(Version.V_8_7_0, false));
    }

    public void testResourceValidation() throws Exception {

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (ReferenceDocs link : ReferenceDocs.values()) {
                builder.field(link.name(), "TEST");
            }
            builder.endObject();

            var map = ReferenceDocs.readLinksBySymbol(BytesReference.bytes(builder).streamInput());
            assertEquals(ReferenceDocs.values().length, map.size());
            for (ReferenceDocs link : ReferenceDocs.values()) {
                assertEquals("TEST", map.get(link.name()));
            }
        }

        try (var stream = new ByteArrayInputStream("{\"invalid\":".getBytes(StandardCharsets.UTF_8))) {
            expectThrows(XContentParseException.class, () -> ReferenceDocs.readLinksBySymbol(stream));
        }

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (ReferenceDocs link : ReferenceDocs.values()) {
                builder.field(link.name(), "TEST");
            }
            builder.startObject("UNEXPECTED").endObject().endObject();

            try (var stream = BytesReference.bytes(builder).streamInput()) {
                expectThrows(IllegalStateException.class, () -> ReferenceDocs.readLinksBySymbol(stream));
            }
        }

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (ReferenceDocs link : ReferenceDocs.values()) {
                builder.field(link.name(), "TEST");
            }
            builder.field("EXTRA", "TEST").endObject();

            try (var stream = BytesReference.bytes(builder).streamInput()) {
                expectThrows(IllegalStateException.class, () -> ReferenceDocs.readLinksBySymbol(stream));
            }
        }

        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            var skipped = randomFrom(ReferenceDocs.values());
            for (ReferenceDocs link : ReferenceDocs.values()) {
                if (link != skipped) {
                    builder.field(link.name(), "TEST");
                }
            }
            builder.endObject();

            try (var stream = BytesReference.bytes(builder).streamInput()) {
                expectThrows(IllegalStateException.class, () -> ReferenceDocs.readLinksBySymbol(stream));
            }
        }

        try (var builder = XContentFactory.jsonBuilder()) {
            var shuffled = Arrays.copyOf(ReferenceDocs.values(), ReferenceDocs.values().length);
            var i = between(0, ReferenceDocs.values().length - 1);
            var j = randomValueOtherThan(i, () -> between(0, ReferenceDocs.values().length - 1));
            var tmp = shuffled[i];
            shuffled[i] = shuffled[j];
            shuffled[j] = tmp;

            builder.startObject();
            for (ReferenceDocs link : shuffled) {
                builder.field(link.name(), "TEST");
            }
            builder.endObject();

            try (var stream = BytesReference.bytes(builder).streamInput()) {
                expectThrows(IllegalStateException.class, () -> ReferenceDocs.readLinksBySymbol(stream));
            }
        }
    }
}
