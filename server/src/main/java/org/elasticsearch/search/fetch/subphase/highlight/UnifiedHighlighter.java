/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.apache.lucene.search.uhighlight.CustomPassageFormatter;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.Snippet;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

public class UnifiedHighlighter implements Highlighter {
    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, CustomUnifiedHighlighter> cache = (Map<String, CustomUnifiedHighlighter>) fieldContext.cache
            .computeIfAbsent(UnifiedHighlighter.class.getName(), k -> new HashMap<>());
        if (cache.containsKey(fieldContext.fieldName) == false) {
            cache.put(fieldContext.fieldName, buildHighlighter(fieldContext));
        }
        CustomUnifiedHighlighter highlighter = cache.get(fieldContext.fieldName);
        MappedFieldType fieldType = fieldContext.fieldType;
        SearchHighlightContext.Field field = fieldContext.field;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;

        CheckedSupplier<String, IOException> loadFieldValues = () -> {
            List<Object> fieldValues = loadFieldValues(highlighter, fieldContext.context.getSearchExecutionContext(), fieldType,
                hitContext, fieldContext.forceSource);
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
            //let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, (o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
    }

    CustomUnifiedHighlighter buildHighlighter(FieldHighlightContext fieldContext) throws IOException {
        Encoder encoder = fieldContext.field.fieldOptions().encoder().equals("html")
            ? HighlightUtils.Encoders.HTML
            : HighlightUtils.Encoders.DEFAULT;
        int maxAnalyzedOffset = fieldContext.context.getSearchExecutionContext().getIndexSettings().getHighlightMaxAnalyzedOffset();
        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments();
        Integer queryMaxAnalyzedOffset = fieldContext.field.fieldOptions().maxAnalyzedOffset();
        Analyzer analyzer = wrapAnalyzer(fieldContext.context.getSearchExecutionContext().getIndexAnalyzer(f -> Lucene.KEYWORD_ANALYZER),
                queryMaxAnalyzedOffset);
        PassageFormatter passageFormatter = getPassageFormatter(fieldContext.hitContext, fieldContext.field, encoder);
        IndexSearcher searcher = fieldContext.context.searcher();
        OffsetSource offsetSource = getOffsetSource(fieldContext.fieldType);
        BreakIterator breakIterator;
        int higlighterNumberOfFragments;
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
            higlighterNumberOfFragments = numberOfFragments == 0 ? Integer.MAX_VALUE - 1 : numberOfFragments;
        } else {
            //using paragraph separator we make sure that each field value holds a discrete passage for highlighting
            breakIterator = getBreakIterator(fieldContext.field);
            higlighterNumberOfFragments = numberOfFragments;
        }
        return new CustomUnifiedHighlighter(
            searcher,
            analyzer,
            offsetSource,
            passageFormatter,
            fieldContext.field.fieldOptions().boundaryScannerLocale(),
            breakIterator,
            fieldContext.context.getIndexName(),
            fieldContext.fieldName,
            fieldContext.query,
            fieldContext.field.fieldOptions().noMatchSize(),
            higlighterNumberOfFragments,
            fieldMatcher(fieldContext),
            maxAnalyzedOffset,
            fieldContext.field.fieldOptions().maxAnalyzedOffset()
        );
    }

    protected PassageFormatter getPassageFormatter(HitContext hitContext, SearchHighlightContext.Field field, Encoder encoder) {
        return new CustomPassageFormatter(field.fieldOptions().preTags()[0],
            field.fieldOptions().postTags()[0], encoder);
    }

    protected Analyzer wrapAnalyzer(Analyzer analyzer, Integer maxAnalyzedOffset) {
        if (maxAnalyzedOffset != null) {
            analyzer = new LimitTokenOffsetAnalyzer(analyzer, maxAnalyzedOffset);
        }
        return analyzer;
    }

    protected List<Object> loadFieldValues(
        CustomUnifiedHighlighter highlighter,
        SearchExecutionContext searchContext,
        MappedFieldType fieldType,
        FetchSubPhase.HitContext hitContext,
        boolean forceSource
    ) throws IOException {
        List<Object> fieldValues = HighlightUtils.loadFieldValues(fieldType, searchContext, hitContext, forceSource);
        fieldValues = fieldValues.stream()
            .map((s) -> convertFieldValue(fieldType, s))
            .collect(Collectors.toList());
        return fieldValues;
    }

    protected BreakIterator getBreakIterator(SearchHighlightContext.Field field) {
        final SearchHighlightContext.FieldOptions fieldOptions = field.fieldOptions();
        final Locale locale =
            fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() :
                Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type =
            fieldOptions.boundaryScannerType()  != null ? fieldOptions.boundaryScannerType() :
                HighlightBuilder.BoundaryScannerType.SENTENCE;
        int maxLen = fieldOptions.fragmentCharSize();
        switch (type) {
            case SENTENCE:
                if (maxLen > 0) {
                    return BoundedBreakIteratorScanner.getSentence(locale, maxLen);
                }
                return BreakIterator.getSentenceInstance(locale);
            case WORD:
                // ignore maxLen
                return BreakIterator.getWordInstance(locale);
            default:
                throw new IllegalArgumentException("Invalid boundary scanner type: " + type.toString());
        }
    }

    protected static String convertFieldValue(MappedFieldType type, Object value) {
        if (value instanceof BytesRef) {
            return type.valueForDisplay(value).toString();
        } else {
            return value.toString();
        }
    }

    protected static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        //postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        //loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }

    protected OffsetSource getOffsetSource(MappedFieldType fieldType) {
        TextSearchInfo tsi = fieldType.getTextSearchInfo();
        if (tsi.hasOffsets()) {
            return tsi.termVectors() != TextSearchInfo.TermVector.NONE
                ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
        }
        if (tsi.termVectors() == TextSearchInfo.TermVector.OFFSETS) {
            return OffsetSource.TERM_VECTORS;
        }
        return OffsetSource.ANALYSIS;
    }

    private Predicate<String> fieldMatcher(FieldHighlightContext fieldContext) {
        if (fieldContext.field.fieldOptions().requireFieldMatch()) {
            String fieldName = fieldContext.fieldName;
            return fieldName::equals;
        }
        // ignore terms that targets the _id field since they use a different encoding
        // that is not compatible with utf8
        return name -> IdFieldMapper.NAME.equals(name) == false;
    }
}
