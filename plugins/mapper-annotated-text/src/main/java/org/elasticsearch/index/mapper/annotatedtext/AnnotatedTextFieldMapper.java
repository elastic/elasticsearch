/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText.AnnotationToken;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

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

    public static final String CONTENT_TYPE = "annotated_text";
    private static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new AnnotatedTextFieldType();
        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        private int positionIncrementGap = POSITION_INCREMENT_GAP_USE_ANALYZER;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public AnnotatedTextFieldType fieldType() {
            return (AnnotatedTextFieldType) super.fieldType();
        }

        public Builder positionIncrementGap(int positionIncrementGap) {
            if (positionIncrementGap < 0) {
                throw new MapperParsingException("[positions_increment_gap] must be positive, got " + positionIncrementGap);
            }
            this.positionIncrementGap = positionIncrementGap;
            return this;
        }

        @Override
        public Builder docValues(boolean docValues) {
            if (docValues) {
                throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields do not support doc values");
            }
            return super.docValues(docValues);
        }

        @Override
        public AnnotatedTextFieldMapper build(BuilderContext context) {
            if (fieldType().indexOptions() == IndexOptions.NONE ) {
                throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields must be indexed");
            }
            if (positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException("Cannot set position_increment_gap on field ["
                        + name + "] without positions enabled");
                }
                fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(), positionIncrementGap));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(), positionIncrementGap));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(), positionIncrementGap));
            } else {
                //Using the analyzer's default BUT need to do the same thing AnalysisRegistry.processAnalyzerFactory
                // does to splice in new default of posIncGap=100 by wrapping the analyzer
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    int overrideInc = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
                    fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(), overrideInc));
                    fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(), overrideInc));
                    fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(),overrideInc));
                }
            }
            setupFieldType(context);
            return new AnnotatedTextFieldMapper(
                    name, fieldType(), defaultFieldType, positionIncrementGap,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<AnnotatedTextFieldMapper.Builder> parse(
                String fieldName, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AnnotatedTextFieldMapper.Builder builder = new AnnotatedTextFieldMapper.Builder(fieldName);

            builder.fieldType().setIndexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.fieldType().setSearchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.fieldType().setSearchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());
            parseTextField(builder, fieldName, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("position_increment_gap")) {
                    int newPositionIncrementGap = XContentMapValues.nodeIntegerValue(propNode, -1);
                    builder.positionIncrementGap(newPositionIncrementGap);
                    iterator.remove();
                }
            }
            return builder;
        }
    }


    /**
     * Parses markdown-like syntax into plain text and AnnotationTokens with offsets for
     * annotations found in texts
     */
    public static final class AnnotatedText {
        public final String textPlusMarkup;
        public final String textMinusMarkup;
        List<AnnotationToken> annotations;

        // Format is markdown-like syntax for URLs eg:
        //   "New mayor is [John Smith](type=person&value=John%20Smith) "
        static Pattern markdownPattern = Pattern.compile("\\[([^\\]\\[]*)\\]\\(([^\\)\\(]*)\\)");

        public static AnnotatedText parse (String textPlusMarkup) {
            List<AnnotationToken> annotations =new ArrayList<>();
            Matcher m = markdownPattern.matcher(textPlusMarkup);
            int lastPos = 0;
            StringBuilder sb = new StringBuilder();
            while(m.find()){
                if(m.start() > lastPos){
                    sb.append(textPlusMarkup.substring(lastPos, m.start()));
                }

                int startOffset = sb.length();
                int endOffset = sb.length() + m.group(1).length();
                sb.append(m.group(1));
                lastPos = m.end();

                String[] pairs = m.group(2).split("&");
                String value = null;
                for (String pair : pairs) {
                    String[] kv = pair.split("=");
                    try {
                        if(kv.length == 2){
                            throw new ElasticsearchParseException("key=value pairs are not supported in annotations");
                        }
                        if(kv.length == 1) {
                            //Check "=" sign wasn't in the pair string
                            if(kv[0].length() == pair.length()) {
                                //untyped value
                                value = URLDecoder.decode(kv[0], "UTF-8");
                            }
                        }
                        if (value!=null && value.length() > 0) {
                            annotations.add(new AnnotationToken(startOffset, endOffset, value));
                        }
                    } catch (UnsupportedEncodingException uee){
                        throw new ElasticsearchParseException("Unsupported encoding parsing annotated text", uee);
                    }
                }
            }
            if(lastPos < textPlusMarkup.length()){
                sb.append(textPlusMarkup.substring(lastPos));
            }
            return new AnnotatedText(sb.toString(), textPlusMarkup, annotations);
        }

        protected AnnotatedText(String textMinusMarkup, String textPlusMarkup, List<AnnotationToken> annotations) {
            this.textMinusMarkup = textMinusMarkup;
            this.textPlusMarkup = textPlusMarkup;
            this.annotations = annotations;
        }

        public static final class AnnotationToken {
            public final int offset;
            public final int endOffset;

            public final String value;
            public AnnotationToken(int offset, int endOffset, String value) {
                this.offset = offset;
                this.endOffset = endOffset;
                this.value = value;
            }
            @Override
            public String toString() {
               return value +" ("+offset+" - "+endOffset+")";
            }

            public boolean intersects(int start, int end) {
                return (start <= offset && end >= offset) || (start <= endOffset && end >= endOffset)
                        || (start >= offset && end <= endOffset);
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + endOffset;
                result = prime * result + offset;
                result = prime * result + Objects.hashCode(value);
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                AnnotationToken other = (AnnotationToken) obj;
                return Objects.equals(endOffset, other.endOffset) && Objects.equals(offset, other.offset)
                        && Objects.equals(value, other.value);
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

    // A utility class for use with highlighters where the content being highlighted
    // needs plain text format for highlighting but marked-up format for token discovery.
    // The class takes markedup format field values and returns plain text versions.
    // When asked to tokenize plain-text versions by the highlighter it tokenizes the
    // original markup form in order to inject annotations.
    public static final class AnnotatedHighlighterAnalyzer extends AnalyzerWrapper {
        private final Analyzer delegate;
        private final HitContext hitContext;
        public AnnotatedHighlighterAnalyzer(Analyzer delegate, HitContext hitContext){
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.hitContext = hitContext;
        }

        @Override
        public Analyzer getWrappedAnalyzer(String fieldName) {
          return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            AnnotationsInjector injector = new AnnotationsInjector(components.getTokenStream());
            AnnotatedText[] annotations = (AnnotatedText[]) hitContext.cache().get(AnnotatedText.class.getName());
            AtomicInteger readerNum = new AtomicInteger(0);
            return new TokenStreamComponents(r -> {
                String plainText = readToString(r);
                AnnotatedText at = annotations[readerNum.getAndIncrement()];
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

        public void setAnnotations(AnnotatedText annotatedText) {
          this.annotatedText = annotatedText;
          currentAnnotationIndex = 0;
          if(annotatedText!=null && annotatedText.numAnnotations()>0){
              nextAnnotationForInjection = annotatedText.getAnnotation(0);
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
        private boolean internalNextToken() throws IOException{
            if (pendingStatePos < pendingStates.size()){
                restoreState(pendingStates.get(pendingStatePos));
                pendingStatePos ++;
                if(pendingStatePos >=pendingStates.size()){
                    pendingStatePos =0;
                    pendingStates.clear();
                }
                return true;
            }
            if(inputExhausted) {
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
        private void setType(AnnotationToken token) {
            //Default annotation type - in future AnnotationTokens may contain custom type info
            typeAtt.setType("annotation");
        }

        private void emitAnnotation(int firstSpannedTextPosInc, int annotationPosLen) throws IOException {
            // Set the annotation's attributes
            posLenAtt.setPositionLength(annotationPosLen);
            textOffsetAtt.setOffset(nextAnnotationForInjection.offset, nextAnnotationForInjection.endOffset);
            setType(nextAnnotationForInjection);

            // We may have multiple annotations at this location - stack them up
            final int annotationOffset = nextAnnotationForInjection.offset;
            final AnnotatedText.AnnotationToken firstAnnotationAtThisPos = nextAnnotationForInjection;
            while (nextAnnotationForInjection != null && nextAnnotationForInjection.offset == annotationOffset) {


                setType(nextAnnotationForInjection);
                termAtt.resizeBuffer(nextAnnotationForInjection.value.length());
                termAtt.copyBuffer(nextAnnotationForInjection.value.toCharArray(), 0, nextAnnotationForInjection.value.length());

                if (nextAnnotationForInjection == firstAnnotationAtThisPos) {
                    posAtt.setPositionIncrement(firstSpannedTextPosInc);
                    //Put at the head of the queue of tokens to be emitted
                    pendingStates.add(0, captureState());
                } else {
                    posAtt.setPositionIncrement(0);
                    //Put after the head of the queue of tokens to be emitted
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

        public AnnotatedTextFieldType() {
            setTokenized(true);
        }

        protected AnnotatedTextFieldType(AnnotatedTextFieldType ref) {
            super(ref);
        }

        @Override
        public void setIndexAnalyzer(NamedAnalyzer delegate) {
            if(delegate.analyzer() instanceof AnnotationAnalyzerWrapper){
                // Already wrapped the Analyzer with an AnnotationAnalyzer
                super.setIndexAnalyzer(delegate);
            } else {
                // Wrap the analyzer with an AnnotationAnalyzer that will inject required annotations
                super.setIndexAnalyzer(new NamedAnalyzer(delegate.name(), AnalyzerScope.INDEX,
                    new AnnotationAnalyzerWrapper(delegate.analyzer())));
            }
        }

        public AnnotatedTextFieldType clone() {
            return new AnnotatedTextFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

    }

    private int positionIncrementGap;
    protected AnnotatedTextFieldMapper(String simpleName, AnnotatedTextFieldType fieldType, MappedFieldType defaultFieldType,
                                int positionIncrementGap,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.tokenized();
        assert fieldType.hasDocValues() == false;
        this.positionIncrementGap = positionIncrementGap;
    }

    @Override
    protected AnnotatedTextFieldMapper clone() {
        return (AnnotatedTextFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    public int getPositionIncrementGap() {
        return this.positionIncrementGap;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), value, fieldType());
            context.doc().add(field);
            if (fieldType().omitNorms()) {
                createFieldNamesField(context);
            }
        }
    }

    @Override
    protected String parseSourceValue(Object value) {
        return value.toString();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public AnnotatedTextFieldType fieldType() {
        return (AnnotatedTextFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        doXContentAnalyzers(builder, includeDefaults);

        if (includeDefaults || positionIncrementGap != POSITION_INCREMENT_GAP_USE_ANALYZER) {
            builder.field("position_increment_gap", positionIncrementGap);
        }
    }
}
