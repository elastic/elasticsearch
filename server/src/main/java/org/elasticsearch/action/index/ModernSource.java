/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.ESONFlat;
import org.elasticsearch.ingest.ESONSource;
import org.elasticsearch.ingest.ESONXContentSerializer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;

public class ModernSource {

    private final XContentType contentType;
    private final int originalSourceSize;
    private BytesReference originalSource;
    private ESONFlat structuredSource;

    public ModernSource(BytesReference originalSource, XContentType contentType) {
        this(originalSource, contentType, originalSource.length(), null);
    }

    public ModernSource(XContentType contentType, int originalSourceSize, ESONFlat structuredSource) {
        this(null, contentType, originalSourceSize, structuredSource);
    }

    public ModernSource(BytesReference originalSource, XContentType contentType, int originalSourceSize, ESONFlat structuredSource) {
        this.originalSource = originalSource;
        this.contentType = contentType;
        this.originalSourceSize = originalSourceSize;
        this.structuredSource = structuredSource;
    }

    public ModernSource(BytesReference source) {
        this(source, XContentHelper.xContentType(source));
    }

    public void ensureStructured() {
        if (structuredSource == null) {
            assert originalSource != null;
            ESONSource.Builder builder = new ESONSource.Builder((int) (originalSource.length() * 0.70));
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, originalSource, contentType)) {
                structuredSource = builder.parse(parser).esonFlat();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public int originalSourceSize() {
        return originalSourceSize;
    }

    public BytesReference originalSourceBytes() {
        if (originalSource == null) {
            assert structuredSource != null;
            try (XContentBuilder builder = XContentFactory.contentBuilder(contentType)) {
                ESONXContentSerializer.flattenToXContent(structuredSource, builder, ToXContent.EMPTY_PARAMS);
                originalSource = BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return originalSource;
    }

    public boolean isStructured() {
        return structuredSource != null;
    }

    public ESONFlat structuredSource() {
        if (structuredSource == null) {
            ensureStructured();
        }
        return structuredSource;
    }
}
