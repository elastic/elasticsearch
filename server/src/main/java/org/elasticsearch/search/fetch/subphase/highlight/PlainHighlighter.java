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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.fetch.subphase.highlight.AbstractHighlighterBuilder.MAX_ANALYZED_OFFSET_FIELD;
import static org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter.convertFieldValue;

public class PlainHighlighter implements Highlighter {
    private static final String CACHE_KEY = "highlight-plain";

    private record OrderedTextFragment(TextFragment textFragment, int fragNum) {
        float score() {
            return textFragment.getScore();
        }

        @Override
        public String toString() {
            return textFragment.toString();
        }
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        SearchHighlightContext.Field field = fieldContext.field;
        FetchContext context = fieldContext.context;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;
        MappedFieldType fieldType = fieldContext.fieldType;

        Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (fieldContext.cache.containsKey(CACHE_KEY) == false) {
            fieldContext.cache.put(CACHE_KEY, new HashMap<>());
        }
        @SuppressWarnings("unchecked")
        Map<MappedFieldType, org.apache.lucene.search.highlight.Highlighter> cache = (Map<
            MappedFieldType,
            org.apache.lucene.search.highlight.Highlighter>) fieldContext.cache.get(CACHE_KEY);

        org.apache.lucene.search.highlight.Highlighter entry = cache.get(fieldType);
        if (entry == null) {
            QueryScorer queryScorer = new CustomQueryScorer(
                fieldContext.query,
                field.fieldOptions().requireFieldMatch() ? fieldType.name() : null
            );
            queryScorer.setExpandMultiTermQuery(true);
            Fragmenter fragmenter;
            if (field.fieldOptions().numberOfFragments() == 0) {
                fragmenter = new NullFragmenter();
            } else if (field.fieldOptions().fragmenter() == null) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else if ("simple".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleFragmenter(field.fieldOptions().fragmentCharSize());
            } else if ("span".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else {
                throw new IllegalArgumentException(
                    "unknown fragmenter option [" + field.fieldOptions().fragmenter() + "] for the field [" + fieldContext.fieldName + "]"
                );
            }
            Formatter formatter = new SimpleHTMLFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0]);

            entry = new org.apache.lucene.search.highlight.Highlighter(formatter, encoder, queryScorer);
            entry.setTextFragmenter(fragmenter);
            // always highlight across all data
            entry.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

            cache.put(fieldType, entry);
        }

        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
        int numberOfFragments = field.fieldOptions().numberOfFragments() == 0 ? 1 : field.fieldOptions().numberOfFragments();
        ArrayList<OrderedTextFragment> fragsList = new ArrayList<>();
        List<Object> textsToHighlight;
        final int maxAnalyzedOffset = context.getSearchExecutionContext().getIndexSettings().getHighlightMaxAnalyzedOffset();
        QueryMaxAnalyzedOffset queryMaxAnalyzedOffset = QueryMaxAnalyzedOffset.create(
            fieldContext.field.fieldOptions().maxAnalyzedOffset(),
            maxAnalyzedOffset
        );
        Analyzer analyzer = wrapAnalyzer(
            context.getSearchExecutionContext().getIndexAnalyzer(f -> Lucene.KEYWORD_ANALYZER),
            queryMaxAnalyzedOffset
        );

        textsToHighlight = HighlightUtils.loadFieldValues(fieldType, context.getSearchExecutionContext(), hitContext);

        int fragNumBase = 0;
        for (Object textToHighlight : textsToHighlight) {
            String text = convertFieldValue(fieldType, textToHighlight);
            int textLength = text.length();
            if ((queryMaxAnalyzedOffset == null || queryMaxAnalyzedOffset.getNotNull() > maxAnalyzedOffset)
                && (textLength > maxAnalyzedOffset)) {
                throw new IllegalArgumentException(
                    "The length ["
                        + textLength
                        + "] of field ["
                        + field
                        + "] in doc["
                        + hitContext.hit().getId()
                        + "]/index["
                        + context.getIndexName()
                        + "] exceeds the ["
                        + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey()
                        + "] "
                        + "limit ["
                        + maxAnalyzedOffset
                        + "]. To avoid this error, set the query parameter ["
                        + MAX_ANALYZED_OFFSET_FIELD.toString()
                        + "] to a value less than index setting ["
                        + maxAnalyzedOffset
                        + "] and "
                        + "this will tolerate long field values by truncating them."
                );
            }

            try (TokenStream tokenStream = analyzer.tokenStream(fieldType.name(), text)) {
                if (tokenStream.hasAttribute(CharTermAttribute.class) == false
                    || tokenStream.hasAttribute(OffsetAttribute.class) == false) {
                    // can't perform highlighting if the stream has no terms (binary token stream) or no offsets
                    continue;
                }
                TextFragment[] bestTextFragments = entry.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                for (TextFragment bestTextFragment : bestTextFragments) {
                    if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                        fragsList.add(new OrderedTextFragment(bestTextFragment, bestTextFragment.getFragNum() + fragNumBase));
                    }
                }
                fragNumBase += bestTextFragments.length;
            } catch (BytesRefHash.MaxBytesLengthExceededException e) {
                // this can happen if for example a field is not_analyzed and ignore_above option is set.
                // the field will be ignored when indexing but the huge term is still in the source and
                // the plain highlighter will parse the source and try to analyze it.
                // ignore and continue to the next value
            } catch (InvalidTokenOffsetsException e) {
                throw new IllegalArgumentException(e);
            }
        }

        // For single text inputs, the fragments are already ordered by score. If we have multiple
        // inputs, or if we are ordering by fragment number, then we need to resort the output list
        if (textsToHighlight.size() > 1 || field.fieldOptions().scoreOrdered() == false) {
            Comparator<OrderedTextFragment> comparator = field.fieldOptions().scoreOrdered()
                ? Comparator.comparingDouble(OrderedTextFragment::score).reversed()
                : Comparator.comparingInt(OrderedTextFragment::fragNum);
            fragsList.sort(comparator);
        }

        String[] fragments;
        // number_of_fragments is set to 0 but we have a multivalued field
        if (field.fieldOptions().numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
            fragments = new String[fragsList.size()];
            for (int i = 0; i < fragsList.size(); i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        } else {
            // refine numberOfFragments if needed
            numberOfFragments = Math.min(fragsList.size(), numberOfFragments);
            fragments = new String[numberOfFragments];
            for (int i = 0; i < fragments.length; i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        }

        if (fragments.length > 0) {
            return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
        }

        int noMatchSize = fieldContext.field.fieldOptions().noMatchSize();
        if (noMatchSize > 0 && textsToHighlight.size() > 0) {
            // Pull an excerpt from the beginning of the string but make sure to split the string on a term boundary.
            String fieldContents = textsToHighlight.get(0).toString();
            int end = findGoodEndForNoHighlightExcerpt(noMatchSize, analyzer, fieldType.name(), fieldContents);
            if (end > 0) {
                return new HighlightField(fieldContext.fieldName, new Text[] { new Text(fieldContents.substring(0, end)) });
            }
        }
        return null;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    private static int findGoodEndForNoHighlightExcerpt(int noMatchSize, Analyzer analyzer, String fieldName, String contents)
        throws IOException {
        try (TokenStream tokenStream = analyzer.tokenStream(fieldName, contents)) {
            if (tokenStream.hasAttribute(OffsetAttribute.class) == false) {
                // Can't split on term boundaries without offsets
                return -1;
            }
            if (contents.length() <= noMatchSize) {
                return contents.length();
            }
            int end = -1;
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                OffsetAttribute attr = tokenStream.getAttribute(OffsetAttribute.class);
                if (attr.endOffset() >= noMatchSize) {
                    // Jump to the end of this token if it wouldn't put us past the boundary
                    if (attr.endOffset() == noMatchSize) {
                        end = noMatchSize;
                    }
                    return end;
                }
                end = attr.endOffset();
            }
            tokenStream.end();
            // We've exhausted the token stream so we should just highlight everything.
            return end;
        }
    }

    private static Analyzer wrapAnalyzer(Analyzer analyzer, QueryMaxAnalyzedOffset maxAnalyzedOffset) {
        if (maxAnalyzedOffset != null) {
            return new LimitTokenOffsetAnalyzer(analyzer, maxAnalyzedOffset.getNotNull());
        }
        return analyzer;
    }
}
