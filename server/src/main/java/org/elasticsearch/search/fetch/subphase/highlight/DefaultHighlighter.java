/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.Builder;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

public class DefaultHighlighter implements Highlighter {

    public static final String NAME = "unified";

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, CustomUnifiedHighlighter> cache = (Map<String, CustomUnifiedHighlighter>) fieldContext.cache.computeIfAbsent(
            UnifiedHighlighter.class.getName(),
            k -> new HashMap<>()
        );
        if (cache.containsKey(fieldContext.fieldName) == false) {
            cache.put(fieldContext.fieldName, buildHighlighter(fieldContext));
        }
        CustomUnifiedHighlighter highlighter = cache.get(fieldContext.fieldName);
        MappedFieldType fieldType = fieldContext.fieldType;
        SearchHighlightContext.Field field = fieldContext.field;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;

        CheckedSupplier<String, IOException> loadFieldValues = () -> {
            List<Object> fieldValues = loadFieldValues(
                highlighter,
                fieldContext.context.getSearchExecutionContext(),
                fieldType,
                hitContext
            );
            if (fieldValues.size() == 0) {
                return null;
            }
            return mergeFieldValues(fieldValues, MULTIVAL_SEP_CHAR);
        };
        Snippet[] fieldSnippets = highlighter.highlightField(hitContext.reader(), hitContext.docId(), loadFieldValues);

        if (fieldSnippets == null || fieldSnippets.length == 0) {
            return null;
        }
        List<Snippet> snippets = new ArrayList<>(fieldSnippets.length);
        for (Snippet fieldSnippet : fieldSnippets) {
            if (Strings.hasText(fieldSnippet.getText())) {
                snippets.add(fieldSnippet);
            }
        }
        if (snippets.isEmpty()) {
            return null;
        }

        if (field.fieldOptions().scoreOrdered()) {
            // let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, (o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
    }

    CustomUnifiedHighlighter buildHighlighter(FieldHighlightContext fieldContext) {
        IndexSettings indexSettings = fieldContext.context.getSearchExecutionContext().getIndexSettings();
        Encoder encoder = fieldContext.field.fieldOptions().encoder().equals("html")
            ? HighlightUtils.Encoders.HTML
            : HighlightUtils.Encoders.DEFAULT;

        int maxAnalyzedOffset = indexSettings.getHighlightMaxAnalyzedOffset();
        boolean weightMatchesEnabled = indexSettings.isWeightMatchesEnabled();
        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments();
        QueryMaxAnalyzedOffset queryMaxAnalyzedOffset = QueryMaxAnalyzedOffset.create(
            fieldContext.field.fieldOptions().maxAnalyzedOffset(),
            maxAnalyzedOffset
        );
        Analyzer analyzer = wrapAnalyzer(
            fieldContext.context.getSearchExecutionContext().getIndexAnalyzer(f -> Lucene.KEYWORD_ANALYZER),
            queryMaxAnalyzedOffset
        );
        PassageFormatter passageFormatter = getPassageFormatter(fieldContext.field, encoder);
        IndexSearcher searcher = fieldContext.context.searcher();
        OffsetSource offsetSource = getOffsetSource(fieldContext.context, fieldContext.fieldType);
        BreakIterator breakIterator;
        int highlighterNumberOfFragments;
        if (numberOfFragments == 0
            // non-tokenized fields should not use any break iterator (ignore boundaryScannerType)
            || fieldContext.fieldType.getTextSearchInfo().isTokenized() == false) {
            /*
             * We use a control char to separate values, which is the
             * only char that the custom break iterator breaks the text
             * on, so we don't lose the distinction between the different
             * values of a field and we get back a snippet per value
             */
            breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
            highlighterNumberOfFragments = numberOfFragments == 0 ? Integer.MAX_VALUE - 1 : numberOfFragments;
        } else {
            // using paragraph separator we make sure that each field value holds a discrete passage for highlighting
            breakIterator = getBreakIterator(fieldContext.field);
            highlighterNumberOfFragments = numberOfFragments;
        }
        Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withBreakIterator(() -> breakIterator);
        builder.withFormatter(passageFormatter);

        Set<String> matchedFields = fieldContext.field.fieldOptions().matchedFields();
        if (matchedFields != null && matchedFields.isEmpty() == false) {
            // Masked fields require that the default field matcher is used
            if (fieldContext.field.fieldOptions().requireFieldMatch() == false) {
                throw new IllegalArgumentException("Matched fields are not supported when [require_field_match] is set to [false]");
            }
            builder.withMaskedFieldsFunc((fieldName) -> fieldName.equals(fieldContext.fieldName) ? matchedFields : Collections.emptySet());
        } else {
            builder.withFieldMatcher(fieldMatcher(fieldContext));
        }
        return new CustomUnifiedHighlighter(
            builder,
            offsetSource,
            fieldContext.field.fieldOptions().boundaryScannerLocale(),
            fieldContext.context.getIndexName(),
            fieldContext.fieldName,
            fieldContext.query,
            fieldContext.field.fieldOptions().noMatchSize(),
            highlighterNumberOfFragments,
            maxAnalyzedOffset,
            queryMaxAnalyzedOffset,
            fieldContext.field.fieldOptions().requireFieldMatch(),
            weightMatchesEnabled
        );
    }

    protected PassageFormatter getPassageFormatter(SearchHighlightContext.Field field, Encoder encoder) {
        return new CustomPassageFormatter(
            field.fieldOptions().preTags()[0],
            field.fieldOptions().postTags()[0],
            encoder,
            field.fieldOptions().numberOfFragments()
        );
    }

    protected Analyzer wrapAnalyzer(Analyzer analyzer, QueryMaxAnalyzedOffset maxAnalyzedOffset) {
        if (maxAnalyzedOffset != null) {
            analyzer = new LimitTokenOffsetAnalyzer(analyzer, maxAnalyzedOffset.getNotNull());
        }
        return analyzer;
    }

    protected List<Object> loadFieldValues(
        CustomUnifiedHighlighter highlighter,
        SearchExecutionContext searchContext,
        MappedFieldType fieldType,
        FetchSubPhase.HitContext hitContext
    ) throws IOException {
        return HighlightUtils.loadFieldValues(fieldType, searchContext, hitContext)
            .stream()
            .<Object>map((s) -> convertFieldValue(fieldType, s))
            .toList();
    }

    protected static BreakIterator getBreakIterator(SearchHighlightContext.Field field) {
        final SearchHighlightContext.FieldOptions fieldOptions = field.fieldOptions();
        final Locale locale = fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() : Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type = fieldOptions.boundaryScannerType() != null
            ? fieldOptions.boundaryScannerType()
            : HighlightBuilder.BoundaryScannerType.SENTENCE;
        int maxLen = fieldOptions.fragmentCharSize();
        switch (type) {
            case SENTENCE -> {
                if (maxLen > 0) {
                    return BoundedBreakIteratorScanner.getSentence(locale, maxLen);
                }
                return BreakIterator.getSentenceInstance(locale);
            }
            case WORD -> {
                // ignore maxLen
                return BreakIterator.getWordInstance(locale);
            }
            default -> throw new IllegalArgumentException("Invalid boundary scanner type: " + type);
        }
    }

    public static String convertFieldValue(MappedFieldType type, Object value) {
        if (value instanceof BytesRef) {
            return type.valueForDisplay(value).toString();
        } else {
            return value.toString();
        }
    }

    public static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        // postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        // loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }

    protected static OffsetSource getOffsetSource(FetchContext fetchContext, MappedFieldType fieldType) {
        if (fetchContext.sourceLoader().reordersFieldValues()) {
            return OffsetSource.ANALYSIS;
        }
        TextSearchInfo tsi = fieldType.getTextSearchInfo();
        if (tsi.hasOffsets()) {
            return tsi.termVectors() != TextSearchInfo.TermVector.NONE ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
        }
        if (tsi.termVectors() == TextSearchInfo.TermVector.OFFSETS) {
            return OffsetSource.TERM_VECTORS;
        }
        return OffsetSource.ANALYSIS;
    }

    private static Predicate<String> fieldMatcher(FieldHighlightContext fieldContext) {
        if (fieldContext.field.fieldOptions().requireFieldMatch()) {
            String fieldName = fieldContext.fieldName;
            return fieldName::equals;
        }
        // ignore terms that targets the _id field since they use a different encoding
        // that is not compatible with utf8
        return name -> IdFieldMapper.NAME.equals(name) == false;
    }
}
