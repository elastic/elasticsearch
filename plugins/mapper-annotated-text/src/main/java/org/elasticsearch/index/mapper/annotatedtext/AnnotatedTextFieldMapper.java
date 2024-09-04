/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.StringStoredFieldFieldLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A {@link FieldMapper} for full-text fields with annotation markup e.g.
 *
 *    "New mayor is [John Smith](type=person&amp;value=John%20Smith) "
 *
 * A special Analyzer wraps the default choice of analyzer in order
 * to strip the text field of annotation markup and inject the related
 * entity annotation tokens as supplementary tokens at the relevant points
 * in the token stream.
 * This code is largely a copy of TextFieldMapper which is less than ideal -
 * my attempts to subclass TextFieldMapper failed but we can revisit this.
 **/
public class AnnotatedTextFieldMapper extends FieldMapper {

    public static final NodeFeature SYNTHETIC_SOURCE_SUPPORT = new NodeFeature("mapper.annotated_text.synthetic_source");

    public static final String CONTENT_TYPE = "annotated_text";

    private static Builder builder(FieldMapper in) {
        return ((AnnotatedTextFieldMapper) in).builder;
    }

    private static NamedAnalyzer wrapAnalyzer(NamedAnalyzer in) {
        return new NamedAnalyzer(
            in.name(),
            AnalyzerScope.INDEX,
            new AnnotationAnalyzerWrapper(in.analyzer()),
            in.getPositionIncrementGap("")
        );
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> builder(m).similarity.getValue());
        final Parameter<String> indexOptions = TextParams.textIndexOptions(m -> builder(m).indexOptions.getValue());
        final Parameter<Boolean> norms = TextParams.norms(true, m -> builder(m).norms.getValue());
        final Parameter<String> termVectors = TextParams.termVectors(m -> builder(m).termVectors.getValue());

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final IndexVersion indexCreatedVersion;
        private final TextParams.Analyzers analyzers;
        private final boolean isSyntheticSourceEnabledViaIndexMode;
        private final Parameter<Boolean> store;

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexAnalyzers indexAnalyzers,
            boolean isSyntheticSourceEnabledViaIndexMode
        ) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(
                indexAnalyzers,
                m -> builder(m).analyzers.getIndexAnalyzer(),
                m -> builder(m).analyzers.positionIncrementGap.getValue(),
                indexCreatedVersion
            );
            this.isSyntheticSourceEnabledViaIndexMode = isSyntheticSourceEnabledViaIndexMode;
            this.store = Parameter.storeParam(
                m -> builder(m).store.getValue(),
                () -> isSyntheticSourceEnabledViaIndexMode && multiFieldsBuilder.hasSyntheticSourceCompatibleKeywordField() == false
            );
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                store,
                indexOptions,
                norms,
                termVectors,
                similarity,
                analyzers.indexAnalyzer,
                analyzers.searchAnalyzer,
                analyzers.searchQuoteAnalyzer,
                analyzers.positionIncrementGap,
                meta };
        }

        private AnnotatedTextFieldType buildFieldType(FieldType fieldType, MapperBuilderContext context, MultiFields multiFields) {
            TextSearchInfo tsi = new TextSearchInfo(
                fieldType,
                similarity.get(),
                wrapAnalyzer(analyzers.getSearchAnalyzer()),
                wrapAnalyzer(analyzers.getSearchQuoteAnalyzer())
            );
            return new AnnotatedTextFieldType(
                context.buildFullName(leafName()),
                store.getValue(),
                tsi,
                context.isSourceSynthetic(),
                TextFieldMapper.SyntheticSourceHelper.syntheticSourceDelegate(fieldType, multiFields),
                meta.getValue()
            );
        }

        @Override
        public AnnotatedTextFieldMapper build(MapperBuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            FieldType fieldType = TextParams.buildFieldType(() -> true, store, indexOptions, norms, termVectors);
            if (fieldType.indexOptions() == IndexOptions.NONE) {
                throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields must be indexed");
            }
            if (analyzers.positionIncrementGap.isConfigured()) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException(
                        "Cannot set position_increment_gap on field [" + leafName() + "] without positions enabled"
                    );
                }
            }
            return new AnnotatedTextFieldMapper(
                leafName(),
                fieldType,
                buildFieldType(fieldType, context, multiFields),
                multiFields,
                copyTo,
                this
            );
        }
    }

    public static TypeParser PARSER = new TypeParser(
        (n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers(), c.getIndexSettings().getMode().isSyntheticSourceEnabled())
    );

    /**
     * Parses markdown-like syntax into plain text and AnnotationTokens with offsets for
     * annotations found in texts
     */
    public record AnnotatedText(String textMinusMarkup, String textPlusMarkup, List<AnnotationToken> annotations) {

        // Format is markdown-like syntax for URLs eg:
        // "New mayor is [John Smith](type=person&value=John%20Smith) "
        static Pattern markdownPattern = Pattern.compile("\\[([^]\\[]*)]\\(([^)(]*)\\)");

        public static AnnotatedText parse(String textPlusMarkup) {
            List<AnnotationToken> annotations = new ArrayList<>();
            Matcher m = markdownPattern.matcher(textPlusMarkup);
            int lastPos = 0;
            StringBuilder sb = new StringBuilder();
            while (m.find()) {
                if (m.start() > lastPos) {
                    sb.append(textPlusMarkup, lastPos, m.start());
                }

                int startOffset = sb.length();
                int endOffset = sb.length() + m.group(1).length();
                sb.append(m.group(1));
                lastPos = m.end();

                String[] pairs = m.group(2).split("&");
                String value = null;
                for (String pair : pairs) {
                    String[] kv = pair.split("=");
                    if (kv.length == 2) {
                        throw new ElasticsearchParseException("key=value pairs are not supported in annotations");
                    }
                    if (kv.length == 1) {
                        // Check "=" sign wasn't in the pair string
                        if (kv[0].length() == pair.length()) {
                            // untyped value
                            value = URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
                        }
                    }
                    if (value != null && value.length() > 0) {
                        annotations.add(new AnnotationToken(startOffset, endOffset, value));
                    }
                }
            }
            if (lastPos < textPlusMarkup.length()) {
                sb.append(textPlusMarkup.substring(lastPos));
            }
            return new AnnotatedText(sb.toString(), textPlusMarkup, annotations);
        }

        public record AnnotationToken(int offset, int endOffset, String value) {

            @Override
            public String toString() {
                return value + " (" + offset + " - " + endOffset + ")";
            }

            public boolean intersects(int start, int end) {
                return (start <= offset && end >= offset)
                    || (start <= endOffset && end >= endOffset)
                    || (start >= offset && end <= endOffset);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(textMinusMarkup);
            sb.append("\n");
            annotations.forEach(a -> {
                sb.append(a);
                sb.append("\n");
            });
            return sb.toString();
        }

        public int numAnnotations() {
            return annotations.size();
        }

        public AnnotationToken getAnnotation(int index) {
            return annotations.get(index);
        }
    }

    /**
     * A utility class for use with highlighters where the content being highlighted
     * needs plain text format for highlighting but marked-up format for token discovery.
     * The class takes marked up format field values and returns plain text versions.
     * When asked to tokenize plain-text versions by the highlighter it tokenizes the
     * original markup form in order to inject annotations.
     * WARNING - not thread safe.
     * Unlike other Analyzers, which tend to be single-instance, this class has
     * instances created per search request and field being highlighted. This allows us to
     * keep state about the annotations being processed and pass them into token streams
     * being highlighted.
     */
    public static final class AnnotatedHighlighterAnalyzer extends AnalyzerWrapper {
        private final Analyzer delegate;
        private AnnotatedText[] annotations;
        // If the field has arrays of values this counter is used to keep track of
        // which array element is currently being highlighted.
        int readerNum;

        public AnnotatedHighlighterAnalyzer(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
        }

        @Override
        public Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        // Called with each new doc being highlighted
        public void setAnnotations(AnnotatedText[] annotations) {
            this.annotations = annotations;
            this.readerNum = 0;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            AnnotationsInjector injector = new AnnotationsInjector(components.getTokenStream());
            return new TokenStreamComponents(r -> {
                String plainText = readToString(r);
                AnnotatedText at = annotations[readerNum++];
                assert at.textMinusMarkup.equals(plainText);
                injector.setAnnotations(at);
                components.getSource().accept(new StringReader(at.textMinusMarkup));
            }, injector);
        }
    }

    public static final class AnnotationAnalyzerWrapper extends AnalyzerWrapper {

        private final Analyzer delegate;

        public AnnotationAnalyzerWrapper(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
        }

        @Override
        public Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            if (components.getTokenStream() instanceof AnnotationsInjector) {
                // already wrapped
                return components;
            }
            AnnotationsInjector injector = new AnnotationsInjector(components.getTokenStream());
            return new TokenStreamComponents(r -> {
                AnnotatedText annotations = AnnotatedText.parse(readToString(r));
                injector.setAnnotations(annotations);
                components.getSource().accept(new StringReader(annotations.textMinusMarkup));
            }, injector);
        }
    }

    static String readToString(Reader reader) {
        char[] arr = new char[8 * 1024];
        StringBuilder buffer = new StringBuilder();
        int numCharsRead;
        try {
            while ((numCharsRead = reader.read(arr, 0, arr.length)) != -1) {
                buffer.append(arr, 0, numCharsRead);
            }
            reader.close();
            return buffer.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("IO Error reading field content", e);
        }
    }

    public static final class AnnotationsInjector extends TokenFilter {

        private AnnotatedText annotatedText;
        AnnotatedText.AnnotationToken nextAnnotationForInjection = null;
        private int currentAnnotationIndex = 0;
        List<State> pendingStates = new ArrayList<>();
        int pendingStatePos = 0;
        boolean inputExhausted = false;

        private final OffsetAttribute textOffsetAtt = addAttribute(OffsetAttribute.class);
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
        private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
        private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

        public AnnotationsInjector(TokenStream in) {
            super(in);
        }

        public void setAnnotations(AnnotatedText text) {
            this.annotatedText = text;
            currentAnnotationIndex = 0;
            if (text != null && text.numAnnotations() > 0) {
                nextAnnotationForInjection = text.getAnnotation(0);
            } else {
                nextAnnotationForInjection = null;
            }
        }

        @Override
        public void reset() throws IOException {
            pendingStates.clear();
            pendingStatePos = 0;
            inputExhausted = false;
            super.reset();
        }

        // Abstracts if we are pulling from some pre-cached buffer of
        // text tokens or directly from the wrapped TokenStream
        private boolean internalNextToken() throws IOException {
            if (pendingStatePos < pendingStates.size()) {
                restoreState(pendingStates.get(pendingStatePos));
                pendingStatePos++;
                if (pendingStatePos >= pendingStates.size()) {
                    pendingStatePos = 0;
                    pendingStates.clear();
                }
                return true;
            }
            if (inputExhausted) {
                return false;
            }
            return input.incrementToken();
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (internalNextToken()) {
                if (nextAnnotationForInjection != null) {
                    // If we are at the right point to inject an annotation....
                    if (textOffsetAtt.startOffset() >= nextAnnotationForInjection.offset) {
                        int firstSpannedTextPosInc = posAtt.getPositionIncrement();
                        int annotationPosLen = 1;

                        // Capture the text token's state for later replay - but
                        // with a zero pos increment so is same as annotation
                        // that is injected before it
                        posAtt.setPositionIncrement(0);
                        pendingStates.add(captureState());

                        while (textOffsetAtt.endOffset() <= nextAnnotationForInjection.endOffset) {
                            // Buffer up all the other tokens spanned by this annotation to determine length.
                            if (input.incrementToken()) {
                                if (textOffsetAtt.endOffset() <= nextAnnotationForInjection.endOffset
                                    && textOffsetAtt.startOffset() < nextAnnotationForInjection.endOffset) {
                                    annotationPosLen += posAtt.getPositionIncrement();
                                }
                                pendingStates.add(captureState());
                            } else {
                                inputExhausted = true;
                                break;
                            }
                        }
                        emitAnnotation(firstSpannedTextPosInc, annotationPosLen);
                        return true;
                    }
                }
                return true;
            } else {
                inputExhausted = true;
                return false;
            }
        }

        private void setType() {
            // Default annotation type - in future AnnotationTokens may contain custom type info
            typeAtt.setType("annotation");
        }

        private void emitAnnotation(int firstSpannedTextPosInc, int annotationPosLen) throws IOException {
            // Set the annotation's attributes
            posLenAtt.setPositionLength(annotationPosLen);
            textOffsetAtt.setOffset(nextAnnotationForInjection.offset, nextAnnotationForInjection.endOffset);
            setType();

            // We may have multiple annotations at this location - stack them up
            final int annotationOffset = nextAnnotationForInjection.offset;
            final AnnotatedText.AnnotationToken firstAnnotationAtThisPos = nextAnnotationForInjection;
            while (nextAnnotationForInjection != null && nextAnnotationForInjection.offset == annotationOffset) {

                setType();
                termAtt.resizeBuffer(nextAnnotationForInjection.value.length());
                termAtt.copyBuffer(nextAnnotationForInjection.value.toCharArray(), 0, nextAnnotationForInjection.value.length());

                if (nextAnnotationForInjection == firstAnnotationAtThisPos) {
                    posAtt.setPositionIncrement(firstSpannedTextPosInc);
                    // Put at the head of the queue of tokens to be emitted
                    pendingStates.add(0, captureState());
                } else {
                    posAtt.setPositionIncrement(0);
                    // Put after the head of the queue of tokens to be emitted
                    pendingStates.add(1, captureState());
                }

                // Flag the inject annotation as null to prevent re-injection.
                currentAnnotationIndex++;
                if (currentAnnotationIndex < annotatedText.numAnnotations()) {
                    nextAnnotationForInjection = annotatedText.getAnnotation(currentAnnotationIndex);
                } else {
                    nextAnnotationForInjection = null;
                }
            }
            // Now pop the first of many potential buffered tokens:
            internalNextToken();
        }

    }

    public static final class AnnotatedTextFieldType extends TextFieldMapper.TextFieldType {
        private AnnotatedTextFieldType(
            String name,
            boolean store,
            TextSearchInfo tsi,
            boolean isSyntheticSource,
            KeywordFieldMapper.KeywordFieldType syntheticSourceDelegate,
            Map<String, String> meta
        ) {
            super(name, true, store, tsi, isSyntheticSource, syntheticSourceDelegate, meta, false, false);
        }

        public AnnotatedTextFieldType(String name, Map<String, String> meta) {
            super(name, true, false, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }
    }

    private final FieldType fieldType;
    private final Builder builder;

    private final NamedAnalyzer indexAnalyzer;

    protected AnnotatedTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        AnnotatedTextFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        assert fieldType.tokenized();
        this.fieldType = freezeAndDeduplicateFieldType(fieldType);
        this.builder = builder;
        this.indexAnalyzer = wrapAnalyzer(builder.analyzers.getIndexAnalyzer());
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), indexAnalyzer);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(mappedFieldType.name(), value, fieldType);
            context.doc().add(field);
            if (fieldType.omitNorms()) {
                context.addToFieldNames(fieldType().name());
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(
            leafName(),
            builder.indexCreatedVersion,
            builder.analyzers.indexAnalyzers,
            builder.isSyntheticSourceEnabledViaIndexMode
        ).init(this);
    }

    @Override
    protected SyntheticSourceMode syntheticSourceMode() {
        return SyntheticSourceMode.NATIVE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        if (copyTo.copyToFields().isEmpty() != true) {
            throw new IllegalArgumentException(
                "field [" + fullPath() + "] of type [" + typeName() + "] doesn't support synthetic source because it declares copy_to"
            );
        }
        if (fieldType.stored()) {
            return new StringStoredFieldFieldLoader(fullPath(), leafName()) {
                @Override
                protected void write(XContentBuilder b, Object value) throws IOException {
                    b.value((String) value);
                }
            };
        }

        var kwd = TextFieldMapper.SyntheticSourceHelper.getKeywordFieldMapperForSyntheticSource(this);
        if (kwd != null) {
            return kwd.syntheticFieldLoader(leafName());
        }

        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "field [%s] of type [%s] doesn't support synthetic source unless it is stored or has a sub-field of"
                    + " type [keyword] with doc values or stored and without a normalizer",
                fullPath(),
                typeName()
            )
        );
    }
}
