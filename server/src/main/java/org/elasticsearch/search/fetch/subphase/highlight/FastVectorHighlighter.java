/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.BreakIteratorBoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.apache.lucene.search.vectorhighlight.SimpleFieldFragList;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;
import org.apache.lucene.search.vectorhighlight.SingleFragListBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.lucene.search.vectorhighlight.CustomFieldQuery;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext.Field;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext.FieldOptions;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class FastVectorHighlighter implements Highlighter {
    private static final BoundaryScanner DEFAULT_SIMPLE_BOUNDARY_SCANNER = new SimpleBoundaryScanner();
    private static final BoundaryScanner DEFAULT_SENTENCE_BOUNDARY_SCANNER = new BreakIteratorBoundaryScanner(
        BreakIterator.getSentenceInstance(Locale.ROOT)
    );
    private static final BoundaryScanner DEFAULT_WORD_BOUNDARY_SCANNER = new BreakIteratorBoundaryScanner(
        BreakIterator.getWordInstance(Locale.ROOT)
    );

    public static final Setting<Boolean> SETTING_TV_HIGHLIGHT_MULTI_VALUE = Setting.boolSetting(
        "search.highlight.term_vector_multi_value",
        true,
        Setting.Property.NodeScope
    );

    private static final String CACHE_KEY = "highlight-fsv";
    private final Boolean termVectorMultiValue;

    public FastVectorHighlighter(Settings settings) {
        this.termVectorMultiValue = SETTING_TV_HIGHLIGHT_MULTI_VALUE.get(settings);
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        SearchHighlightContext.Field field = fieldContext.field;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;
        MappedFieldType fieldType = fieldContext.fieldType;
        boolean fixBrokenAnalysis = fieldContext.context.containsBrokenAnalysis(fieldContext.fieldName);

        if (canHighlight(fieldType) == false) {
            throw new IllegalArgumentException(
                "the field ["
                    + fieldContext.fieldName
                    + "] should be indexed with term vector with position offsets to be used with fast vector highlighter"
            );
        }

        Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (fieldContext.cache.containsKey(CACHE_KEY) == false) {
            fieldContext.cache.put(CACHE_KEY, new HighlighterEntry());
        }
        HighlighterEntry cache = (HighlighterEntry) fieldContext.cache.get(CACHE_KEY);
        FieldHighlightEntry entry = cache.fields.get(fieldType);
        if (entry == null) {
            FragListBuilder fragListBuilder;
            if (field.fieldOptions().numberOfFragments() == 0) {
                fragListBuilder = new SingleFragListBuilder();
            } else {
                fragListBuilder = field.fieldOptions().fragmentOffset() == -1
                    ? new SimpleFragListBuilder()
                    : new SimpleFragListBuilder(field.fieldOptions().fragmentOffset());
            }

            Function<Source, FragmentsBuilder> fragmentsBuilderSupplier = fragmentsBuilderSupplier(
                field,
                fieldType,
                fieldContext.context,
                fixBrokenAnalysis
            );

            entry = new FieldHighlightEntry();
            if (field.fieldOptions().requireFieldMatch()) {
                /*
                 * we use top level reader to rewrite the query against all readers,
                 * with use caching it across hits (and across readers...)
                 */
                entry.fieldMatchFieldQuery = new CustomFieldQuery(
                    fieldContext.query,
                    hitContext.topLevelReader(),
                    true,
                    field.fieldOptions().requireFieldMatch()
                );
            } else {
                /*
                 * we use top level reader to rewrite the query against all readers,
                 * with use caching it across hits (and across readers...)
                 */
                entry.noFieldMatchFieldQuery = new CustomFieldQuery(
                    fieldContext.query,
                    hitContext.topLevelReader(),
                    true,
                    field.fieldOptions().requireFieldMatch()
                );
            }
            entry.fragListBuilder = fragListBuilder;
            entry.fragmentsBuilderSupplier = fragmentsBuilderSupplier;
            if (cache.fvh == null) {
                // parameters to FVH are not requires since:
                // first two booleans are not relevant since they are set on the CustomFieldQuery
                // (phrase and fieldMatch) fragment builders are used explicitly
                cache.fvh = new org.apache.lucene.search.vectorhighlight.FastVectorHighlighter();
            }
            CustomFieldQuery.highlightFilters.set(field.fieldOptions().highlightFilter());
            cache.fields.put(fieldType, entry);
        }
        final FieldQuery fieldQuery;
        if (field.fieldOptions().requireFieldMatch()) {
            fieldQuery = entry.fieldMatchFieldQuery;
        } else {
            fieldQuery = entry.noFieldMatchFieldQuery;
        }
        cache.fvh.setPhraseLimit(field.fieldOptions().phraseLimit());

        String[] fragments;
        FragmentsBuilder fragmentsBuilder = entry.fragmentsBuilderSupplier.apply(hitContext.source());

        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
        int numberOfFragments = field.fieldOptions().numberOfFragments() == 0
            ? Integer.MAX_VALUE
            : field.fieldOptions().numberOfFragments();
        int fragmentCharSize = field.fieldOptions().numberOfFragments() == 0 ? Integer.MAX_VALUE : field.fieldOptions().fragmentCharSize();
        // we highlight against the low level reader and docId, because if we load source, we want to reuse it if possible
        // Only send matched fields if they were requested to save time.
        if (field.fieldOptions().matchedFields() != null && field.fieldOptions().matchedFields().isEmpty() == false) {
            fragments = cache.fvh.getBestFragments(
                fieldQuery,
                hitContext.reader(),
                hitContext.docId(),
                fieldType.name(),
                field.fieldOptions().matchedFields(),
                fragmentCharSize,
                numberOfFragments,
                entry.fragListBuilder,
                fragmentsBuilder,
                field.fieldOptions().preTags(),
                field.fieldOptions().postTags(),
                encoder
            );
        } else {
            fragments = cache.fvh.getBestFragments(
                fieldQuery,
                hitContext.reader(),
                hitContext.docId(),
                fieldType.name(),
                fragmentCharSize,
                numberOfFragments,
                entry.fragListBuilder,
                fragmentsBuilder,
                field.fieldOptions().preTags(),
                field.fieldOptions().postTags(),
                encoder
            );
        }

        if (CollectionUtils.isEmpty(fragments) == false) {
            return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
        }

        int noMatchSize = fieldContext.field.fieldOptions().noMatchSize();
        if (noMatchSize > 0) {
            // Essentially we just request that a fragment is built from 0 to noMatchSize using
            // the normal fragmentsBuilder
            FieldFragList fieldFragList = new SimpleFieldFragList(-1 /*ignored*/);
            fieldFragList.add(0, noMatchSize, Collections.emptyList());
            fragments = fragmentsBuilder.createFragments(
                hitContext.reader(),
                hitContext.docId(),
                fieldType.name(),
                fieldFragList,
                1,
                field.fieldOptions().preTags(),
                field.fieldOptions().postTags(),
                encoder
            );
            if (CollectionUtils.isEmpty(fragments) == false) {
                return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
            }
        }

        return null;
    }

    private Function<Source, FragmentsBuilder> fragmentsBuilderSupplier(
        SearchHighlightContext.Field field,
        MappedFieldType fieldType,
        FetchContext fetchContext,
        boolean fixBrokenAnalysis
    ) {
        BoundaryScanner boundaryScanner = getBoundaryScanner(field);
        FieldOptions options = field.fieldOptions();
        Function<Source, BaseFragmentsBuilder> supplier;
        if (fieldType.isStored()) {
            if (options.numberOfFragments() != 0 && options.scoreOrdered()) {
                supplier = ignored -> new ScoreOrderFragmentsBuilder(options.preTags(), options.postTags(), boundaryScanner);
            } else {
                supplier = ignored -> new SimpleFragmentsBuilder(
                    fieldType,
                    fixBrokenAnalysis,
                    options.preTags(),
                    options.postTags(),
                    boundaryScanner
                );
            }
        } else {
            if (options.numberOfFragments() != 0 && options.scoreOrdered()) {
                supplier = lookup -> new SourceScoreOrderFragmentsBuilder(
                    fieldType,
                    fetchContext,
                    fixBrokenAnalysis,
                    lookup,
                    options.preTags(),
                    options.postTags(),
                    boundaryScanner
                );
            } else {
                supplier = lookup -> new SourceSimpleFragmentsBuilder(
                    fieldType,
                    fetchContext,
                    fixBrokenAnalysis,
                    lookup,
                    options.preTags(),
                    options.postTags(),
                    boundaryScanner
                );
            }
        }

        return lookup -> {
            BaseFragmentsBuilder builder = supplier.apply(lookup);
            builder.setDiscreteMultiValueHighlighting(termVectorMultiValue);
            return builder;
        };
    }

    @Override
    public boolean canHighlight(MappedFieldType ft) {
        return ft.getTextSearchInfo().termVectors() == TextSearchInfo.TermVector.OFFSETS;
    }

    private static BoundaryScanner getBoundaryScanner(Field field) {
        final FieldOptions fieldOptions = field.fieldOptions();
        final Locale boundaryScannerLocale = fieldOptions.boundaryScannerLocale() != null
            ? fieldOptions.boundaryScannerLocale()
            : Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type = fieldOptions.boundaryScannerType() != null
            ? fieldOptions.boundaryScannerType()
            : HighlightBuilder.BoundaryScannerType.CHARS;
        switch (type) {
            case SENTENCE -> {
                if (boundaryScannerLocale != null) {
                    return new BreakIteratorBoundaryScanner(BreakIterator.getSentenceInstance(boundaryScannerLocale));
                }
                return DEFAULT_SENTENCE_BOUNDARY_SCANNER;
            }
            case WORD -> {
                if (boundaryScannerLocale != null) {
                    return new BreakIteratorBoundaryScanner(BreakIterator.getWordInstance(boundaryScannerLocale));
                }
                return DEFAULT_WORD_BOUNDARY_SCANNER;
            }
            case CHARS -> {
                if (fieldOptions.boundaryMaxScan() != SimpleBoundaryScanner.DEFAULT_MAX_SCAN
                    || fieldOptions.boundaryChars() != SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS) {
                    return new SimpleBoundaryScanner(fieldOptions.boundaryMaxScan(), fieldOptions.boundaryChars());
                }
                return DEFAULT_SIMPLE_BOUNDARY_SCANNER;
            }
            default -> throw new IllegalArgumentException("Invalid boundary scanner type: " + type.toString());
        }
    }

    private static class FieldHighlightEntry {
        public FragListBuilder fragListBuilder;
        public Function<Source, FragmentsBuilder> fragmentsBuilderSupplier;
        public FieldQuery noFieldMatchFieldQuery;
        public FieldQuery fieldMatchFieldQuery;
    }

    private static class HighlighterEntry {
        public org.apache.lucene.search.vectorhighlight.FastVectorHighlighter fvh;
        public Map<MappedFieldType, FieldHighlightEntry> fields = new HashMap<>();
    }
}
