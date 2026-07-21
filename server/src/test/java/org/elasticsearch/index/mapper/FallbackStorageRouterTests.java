/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class FallbackStorageRouterTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // route() — pure function, exhaustive coverage over every Reason value
    // -------------------------------------------------------------------------

    public void testMalformedRoutesToIgnoreMalformed() {
        assertThat(
            FallbackStorageRouter.route(FallbackStorageRouter.Reason.MALFORMED),
            is(FallbackStorageRouter.Destination.IGNORE_MALFORMED)
        );
    }

    public void testMultiValueViolationRoutesToOnFailure() {
        assertThat(
            FallbackStorageRouter.route(FallbackStorageRouter.Reason.MULTI_VALUE_VIOLATION),
            is(FallbackStorageRouter.Destination.ON_FAILURE)
        );
    }

    public void testIgnoredSourceReasons() {
        EnumSet<FallbackStorageRouter.Reason> ignoredSourceReasons = EnumSet.of(
            FallbackStorageRouter.Reason.SYNTHETIC_FALLBACK,
            FallbackStorageRouter.Reason.SOURCE_KEEP_ALL,
            FallbackStorageRouter.Reason.SOURCE_KEEP_ARRAYS_IN_ARRAY,
            FallbackStorageRouter.Reason.COPY_TO_DESTINATION,
            FallbackStorageRouter.Reason.DYNAMIC_DISABLED,
            FallbackStorageRouter.Reason.DYNAMIC_RUNTIME,
            FallbackStorageRouter.Reason.OBJECT_DISABLED,
            FallbackStorageRouter.Reason.FIELD_LIMIT_EXCEEDED,
            FallbackStorageRouter.Reason.FIELD_NAME_TOO_LONG
        );
        for (FallbackStorageRouter.Reason reason : ignoredSourceReasons) {
            assertThat(
                "Expected IGNORED_SOURCE for reason " + reason,
                FallbackStorageRouter.route(reason),
                is(FallbackStorageRouter.Destination.IGNORED_SOURCE)
            );
        }
    }

    // -------------------------------------------------------------------------
    // shouldPreCaptureToIgnoredSource() — one test per branch
    //
    // DocumentParserContext is tested via TestDocumentParserContext, which provides
    // a working canAddIgnoredField() backed by a real MappingLookup. canAddIgnoredField()
    // is final, so it cannot be mocked; instead, canAddIgnoredField()=false is produced
    // by a non-synthetic MappingLookup (MappingLookup.EMPTY) and canAddIgnoredField()=true
    // by a synthetic-source lookup built via createSyntheticLookup().
    //
    // FieldMapper.syntheticSourceMode() is final, so mocking it is not possible without
    // the inline mock maker. Concrete anonymous subclasses are used instead: fallbackMapper()
    // returns a mapper whose syntheticSourceMode() returns FALLBACK (the default), and
    // nativeMapper() overrides syntheticSourceSupport() to return NATIVE.
    // -------------------------------------------------------------------------

    /** Short-circuits to false when canAddIgnoredField() is false (non-synthetic source). */
    public void testShouldPreCaptureReturnsFalseWhenCannotAddIgnoredField() {
        // TestDocumentParserContext() uses MappingLookup.EMPTY → isSourceSynthetic=false → canAddIgnoredField=false.
        DocumentParserContext ctx = new TestDocumentParserContext();
        FieldMapper mapper = fallbackMapper("f");

        assertFalse(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.ALL, false));
    }

    /** Short-circuits to false when single-value is enforced and on_failure=ignore. */
    public void testShouldPreCaptureReturnsFalseForSingleValueIgnore() {
        DocumentParserContext ctx = syntheticCtx();
        // A mapper with FALLBACK mode that also enforces single-value with on_failure=ignore.
        FieldMapper mapper = new MinimalFieldMapper("f") {
            @Override
            protected boolean isSingleValueEnforced() {
                return true;
            }

            @Override
            protected DocValuesParameter.Values.OnFailure onFailureBehavior() {
                return DocValuesParameter.Values.OnFailure.IGNORE;
            }
        };

        assertFalse(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.NONE, false));
    }

    /** Returns true when syntheticSourceMode is FALLBACK. */
    public void testShouldPreCaptureTrueForSyntheticFallbackMode() {
        DocumentParserContext ctx = syntheticCtx();
        FieldMapper mapper = fallbackMapper("f");

        assertTrue(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.NONE, false));
    }

    /** Returns true when sourceKeepMode is ALL. */
    public void testShouldPreCaptureTrueForSourceKeepAll() {
        DocumentParserContext ctx = syntheticCtx();
        FieldMapper mapper = nativeMapper("f");

        assertTrue(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.ALL, false));
    }

    /**
     * Returns true when sourceKeepMode=ARRAYS, inside array scope, and the mapper does NOT natively
     * parse arrays. inArrayScope() is package-private and overridden in the anonymous subclass because
     * it reads a field set only via the internal maybeCloneForArray() path.
     */
    public void testShouldPreCaptureTrueForSourceKeepArraysInArrayWithNonArrayMapper() {
        DocumentParserContext ctx = new TestDocumentParserContext(createSyntheticLookup(), null) {
            @Override
            boolean inArrayScope() {
                return true;
            }
        };
        FieldMapper mapper = nativeMapper("f");

        assertTrue(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.ARRAYS, false));
    }

    /** Returns false when sourceKeepMode=ARRAYS, inside array scope, but the mapper DOES natively parse arrays. */
    public void testShouldPreCaptureFalseForSourceKeepArraysWhenMapperParsesArrays() {
        DocumentParserContext ctx = new TestDocumentParserContext(createSyntheticLookup(), null) {
            @Override
            boolean inArrayScope() {
                return true;
            }
        };
        FieldMapper mapper = nativeMapper("f");

        assertFalse(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.ARRAYS, true));
    }

    /** Returns false when sourceKeepMode=ARRAYS but NOT inside array scope. */
    public void testShouldPreCaptureFalseForSourceKeepArraysOutsideArrayScope() {
        DocumentParserContext ctx = syntheticCtx(); // inArrayScope() returns false by default
        FieldMapper mapper = nativeMapper("f");

        assertFalse(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.ARRAYS, false));
    }

    /** Returns true when the field is a copy_to destination and we are not within a copy_to traversal. */
    public void testShouldPreCaptureTrueForCopyToDestination() {
        String fieldPath = "target_field";
        TestDocumentParserContext ctx = syntheticCtx();
        // markFieldAsCopyTo adds the field to copyToFields, so isCopyToDestinationField returns true.
        ctx.markFieldAsCopyTo(fieldPath);
        FieldMapper mapper = nativeMapper(fieldPath); // fullPath() == fieldPath

        assertTrue(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.NONE, false));
    }

    /**
     * Returns false when the field is a copy_to destination but we ARE within a copy_to traversal.
     * isWithinCopyTo() is not final; it returns false in the base class and is overridden here to
     * simulate a copy_to traversal context without wiring through createCopyToContext().
     */
    public void testShouldPreCaptureFalseForCopyToWhenWithinCopyTo() {
        String fieldPath = "target_field";
        DocumentParserContext ctx = new TestDocumentParserContext(createSyntheticLookup(), null) {
            @Override
            public boolean isWithinCopyTo() {
                return true;
            }

            @Override
            public boolean isCopyToDestinationField(String name) {
                return fieldPath.equals(name);
            }
        };
        FieldMapper mapper = nativeMapper(fieldPath);

        assertFalse(FallbackStorageRouter.shouldPreCaptureToIgnoredSource(ctx, mapper, Mapper.SourceKeepMode.NONE, false));
    }

    // -------------------------------------------------------------------------
    // helpers — context factories
    // -------------------------------------------------------------------------

    private static MappingLookup createSyntheticLookup() {
        SourceFieldMapper syntheticSource = new SourceFieldMapper.Builder(null, Settings.EMPTY, false, false, false).setSynthetic().build();
        RootObjectMapper root = new RootObjectMapper.Builder("_doc").build(MapperBuilderContext.root(true, false));
        Mapping mapping = new Mapping(root, new MetadataFieldMapper[] { syntheticSource }, Map.of());
        return MappingLookup.fromMapping(mapping, IndexMode.STANDARD);
    }

    /**
     * Builds a {@link TestDocumentParserContext} backed by a synthetic-source {@link MappingLookup}
     * so that {@link DocumentParserContext#canAddIgnoredField()} returns {@code true} by default.
     */
    private static TestDocumentParserContext syntheticCtx() {
        return new TestDocumentParserContext(createSyntheticLookup(), null);
    }

    // -------------------------------------------------------------------------
    // helpers — field mapper factories
    //
    // FieldMapper.syntheticSourceMode() is final and reads internal state, making
    // it impossible to mock with the subclass mock maker. Concrete anonymous subclasses
    // are used instead: MinimalFieldMapper provides a base, with subclasses controlling
    // syntheticSourceSupport() to return FALLBACK (default) or NATIVE.
    // -------------------------------------------------------------------------

    /** A minimal concrete FieldMapper with no-op parseCreateField and a no-op MappedFieldType. */
    private static class MinimalFieldMapper extends FieldMapper {
        MinimalFieldMapper(String name) {
            super(name, new MappedFieldType(name, IndexType.NONE, false, Map.of()) {
                @Override
                public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String typeName() {
                    return "test";
                }

                @Override
                public Query termQuery(Object value, SearchExecutionContext context) {
                    throw new UnsupportedOperationException();
                }
            }, BuilderParams.empty());
        }

        @Override
        public Builder getMergeBuilder() {
            return null;
        }

        @Override
        public String contentType() {
            return "test";
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) {}
    }

    /** Returns a mapper whose {@code syntheticSourceMode()} returns {@code FALLBACK} (the default). */
    private static FieldMapper fallbackMapper(String name) {
        return new MinimalFieldMapper(name);
    }

    /**
     * Returns a mapper whose {@code syntheticSourceMode()} returns {@code NATIVE}.
     * Overrides {@link FieldMapper#syntheticSourceSupport()} rather than the final
     * {@code syntheticSourceMode()}.
     */
    private static FieldMapper nativeMapper(String name) {
        return new MinimalFieldMapper(name) {
            @Override
            protected SyntheticSourceSupport syntheticSourceSupport() {
                return new SyntheticSourceSupport.Native(() -> null);
            }
        };
    }
}
