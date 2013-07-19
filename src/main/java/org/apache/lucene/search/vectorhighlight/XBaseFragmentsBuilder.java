package org.apache.lucene.search.vectorhighlight;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.XFieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.XFieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.XFieldPhraseList.WeightedPhraseInfo.Toffs;
import org.apache.lucene.util.CollectionUtil;

import java.io.IOException;
import java.util.*;

/**
 * Base FragmentsBuilder implementation that supports colored pre/post
 * tags and multivalued fields.
 * <p>
 * Uses {@link XBoundaryScanner} to determine fragments.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.5 IS OUT
public abstract class XBaseFragmentsBuilder implements XFragmentsBuilder {

  protected String[] preTags, postTags;
  public static final String[] COLORED_PRE_TAGS = {
    "<b style=\"background:yellow\">", "<b style=\"background:lawngreen\">", "<b style=\"background:aquamarine\">",
    "<b style=\"background:magenta\">", "<b style=\"background:palegreen\">", "<b style=\"background:coral\">",
    "<b style=\"background:wheat\">", "<b style=\"background:khaki\">", "<b style=\"background:lime\">",
    "<b style=\"background:deepskyblue\">", "<b style=\"background:deeppink\">", "<b style=\"background:salmon\">",
    "<b style=\"background:peachpuff\">", "<b style=\"background:violet\">", "<b style=\"background:mediumpurple\">",
    "<b style=\"background:palegoldenrod\">", "<b style=\"background:darkkhaki\">", "<b style=\"background:springgreen\">",
    "<b style=\"background:turquoise\">", "<b style=\"background:powderblue\">"
  };
  public static final String[] COLORED_POST_TAGS = { "</b>" };
  private char multiValuedSeparator = ' ';
  private final BoundaryScanner boundaryScanner;
  private boolean discreteMultiValueHighlighting = false;
  
  protected XBaseFragmentsBuilder(){
    this( new String[]{ "<b>" }, new String[]{ "</b>" } );
  }
  
  protected XBaseFragmentsBuilder( String[] preTags, String[] postTags ){
    this(preTags, postTags, new SimpleBoundaryScanner());
  }
  
  protected XBaseFragmentsBuilder(BoundaryScanner boundaryScanner){
    this( new String[]{ "<b>" }, new String[]{ "</b>" }, boundaryScanner );
  }
  
  protected XBaseFragmentsBuilder( String[] preTags, String[] postTags, BoundaryScanner boundaryScanner ){
    this.preTags = preTags;
    this.postTags = postTags;
    this.boundaryScanner = boundaryScanner;
  }
  
  static Object checkTagsArgument( Object tags ){
    if( tags instanceof String ) return tags;
    else if( tags instanceof String[] ) return tags;
    throw new IllegalArgumentException( "type of preTags/postTags must be a String or String[]" );
  }
  
  public abstract List<WeightedFragInfo> getWeightedFragInfoList( List<WeightedFragInfo> src );

  private static final Encoder NULL_ENCODER = new DefaultEncoder();

  @Override
  public String createFragment( IndexReader reader, int docId,
      String fieldName, XFieldFragList fieldFragList ) throws IOException {
    return createFragment( reader, docId, fieldName, fieldFragList,
        preTags, postTags, NULL_ENCODER );
  }

  @Override
  public String[] createFragments( IndexReader reader, int docId,
      String fieldName, XFieldFragList fieldFragList, int maxNumFragments )
      throws IOException {
    return createFragments( reader, docId, fieldName, fieldFragList, maxNumFragments,
        preTags, postTags, NULL_ENCODER );
  }
  
  @Override
  public String createFragment( IndexReader reader, int docId,
      String fieldName, XFieldFragList fieldFragList, String[] preTags, String[] postTags,
      Encoder encoder ) throws IOException {
    String[] fragments = createFragments( reader, docId, fieldName, fieldFragList, 1,
        preTags, postTags, encoder );
    if( fragments == null || fragments.length == 0 ) return null;
    return fragments[0];
  }

  @Override
  public String[] createFragments( IndexReader reader, int docId,
      String fieldName, XFieldFragList fieldFragList, int maxNumFragments,
      String[] preTags, String[] postTags, Encoder encoder ) throws IOException {

    if( maxNumFragments < 0 ) {
      throw new IllegalArgumentException( "maxNumFragments(" + maxNumFragments + ") must be positive number." );
    }

    List<WeightedFragInfo> fragInfos = fieldFragList.getFragInfos();
    Field[] values = getFields( reader, docId, fieldName );
    if( values.length == 0 ) {
      return null;
    }

    if (discreteMultiValueHighlighting && values.length > 1) {
      fragInfos = discreteMultiValueHighlighting(fragInfos, values);
    }

    fragInfos = getWeightedFragInfoList(fragInfos);
    int limitFragments = maxNumFragments < fragInfos.size() ? maxNumFragments : fragInfos.size();
    List<String> fragments = new ArrayList<String>( limitFragments );

    StringBuilder buffer = new StringBuilder();
    int[] nextValueIndex = { 0 };
    for( int n = 0; n < limitFragments; n++ ){
      WeightedFragInfo fragInfo = fragInfos.get( n );
      fragments.add( makeFragment( buffer, nextValueIndex, values, fragInfo, preTags, postTags, encoder ) );
    }
    return fragments.toArray( new String[fragments.size()] );
  }
  
  protected Field[] getFields( IndexReader reader, int docId, final String fieldName) throws IOException {
    // according to javadoc, doc.getFields(fieldName) cannot be used with lazy loaded field???
    final List<Field> fields = new ArrayList<Field>();
    reader.document(docId, new StoredFieldVisitor() {
        
        @Override
        public void stringField(FieldInfo fieldInfo, String value) {
          FieldType ft = new FieldType(TextField.TYPE_STORED);
          ft.setStoreTermVectors(fieldInfo.hasVectors());
          fields.add(new Field(fieldInfo.name, value, ft));
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
          return fieldInfo.name.equals(fieldName) ? Status.YES : Status.NO;
        }
      });
    return fields.toArray(new Field[fields.size()]);
  }

  protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
      String[] preTags, String[] postTags, Encoder encoder ){
    StringBuilder fragment = new StringBuilder();
    final int s = fragInfo.getStartOffset();
    int[] modifiedStartOffset = { s };
    String src = getFragmentSourceMSO( buffer, index, values, s, fragInfo.getEndOffset(), modifiedStartOffset );
    int srcIndex = 0;
    for( SubInfo subInfo : fragInfo.getSubInfos() ){
      for( Toffs to : subInfo.getTermsOffsets() ){
        fragment
          .append( encoder.encodeText( src.substring( srcIndex, to.getStartOffset() - modifiedStartOffset[0] ) ) )
          .append( getPreTag( preTags, subInfo.getSeqnum() ) )
          .append( encoder.encodeText( src.substring( to.getStartOffset() - modifiedStartOffset[0], to.getEndOffset() - modifiedStartOffset[0] ) ) )
          .append( getPostTag( postTags, subInfo.getSeqnum() ) );
        srcIndex = to.getEndOffset() - modifiedStartOffset[0];
      }
    }
    fragment.append( encoder.encodeText( src.substring( srcIndex ) ) );
    return fragment.toString();
  }

  protected String getFragmentSourceMSO( StringBuilder buffer, int[] index, Field[] values,
      int startOffset, int endOffset, int[] modifiedStartOffset ){
    while( buffer.length() < endOffset && index[0] < values.length ){
      buffer.append( values[index[0]++].stringValue() );
      buffer.append( getMultiValuedSeparator() );
    }
    int bufferLength = buffer.length();
    // we added the multi value char to the last buffer, ignore it
    if (values[index[0] - 1].fieldType().tokenized()) {
      bufferLength--;
    }
    int eo = bufferLength < endOffset ? bufferLength : boundaryScanner.findEndOffset( buffer, endOffset );
    modifiedStartOffset[0] = boundaryScanner.findStartOffset( buffer, startOffset );
    return buffer.substring( modifiedStartOffset[0], eo );
  }
  
  protected String getFragmentSource( StringBuilder buffer, int[] index, Field[] values,
      int startOffset, int endOffset ){
    while( buffer.length() < endOffset && index[0] < values.length ){
      buffer.append( values[index[0]].stringValue() );
      buffer.append( multiValuedSeparator );
      index[0]++;
    }
    int eo = buffer.length() < endOffset ? buffer.length() : endOffset;
    return buffer.substring( startOffset, eo );
  }

  protected List<WeightedFragInfo> discreteMultiValueHighlighting(List<WeightedFragInfo> fragInfos, Field[] fields) {
    Map<String, List<WeightedFragInfo>> fieldNameToFragInfos = new HashMap<String, List<WeightedFragInfo>>();
    for (Field field : fields) {
      fieldNameToFragInfos.put(field.name(), new ArrayList<WeightedFragInfo>());
    }

    fragInfos: for (WeightedFragInfo fragInfo : fragInfos) {
      int fieldStart;
      int fieldEnd = 0;
      for (Field field : fields) {
        if (field.stringValue().isEmpty()) {
          fieldEnd++;
          continue;
        }
        fieldStart = fieldEnd;
        fieldEnd += field.stringValue().length() + 1; // + 1 for going to next field with same name.

        if (fragInfo.getStartOffset() >= fieldStart && fragInfo.getEndOffset() >= fieldStart &&
            fragInfo.getStartOffset() <= fieldEnd && fragInfo.getEndOffset() <= fieldEnd) {
          fieldNameToFragInfos.get(field.name()).add(fragInfo);
          continue fragInfos;
        }

        if (fragInfo.getSubInfos().isEmpty()) {
          continue fragInfos;
        }

        Toffs firstToffs = fragInfo.getSubInfos().get(0).getTermsOffsets().get(0);
        if (fragInfo.getStartOffset() >= fieldEnd || firstToffs.getStartOffset() >= fieldEnd) {
          continue;
        }

        int fragStart = fieldStart;
        if (fragInfo.getStartOffset() > fieldStart && fragInfo.getStartOffset() < fieldEnd) {
          fragStart = fragInfo.getStartOffset();
        }

        int fragEnd = fieldEnd;
        if (fragInfo.getEndOffset() > fieldStart && fragInfo.getEndOffset() < fieldEnd) {
          fragEnd = fragInfo.getEndOffset();
        }


        List<SubInfo> subInfos = new ArrayList<SubInfo>();
        WeightedFragInfo weightedFragInfo = new WeightedFragInfo(fragStart, fragEnd, subInfos, fragInfo.getTotalBoost());

        Iterator<SubInfo> subInfoIterator = fragInfo.getSubInfos().iterator();
        while (subInfoIterator.hasNext()) {
          SubInfo subInfo = subInfoIterator.next();
          List<Toffs> toffsList = new ArrayList<Toffs>();
          Iterator<Toffs> toffsIterator = subInfo.getTermsOffsets().iterator();
          while (toffsIterator.hasNext()) {
            Toffs toffs = toffsIterator.next();
            if (toffs.getStartOffset() >= fieldStart && toffs.getEndOffset() <= fieldEnd) {
              toffsList.add(toffs);
              toffsIterator.remove();
            }
          }
          if (!toffsList.isEmpty()) {
            subInfos.add(new SubInfo(subInfo.getText(), toffsList, subInfo.getSeqnum()));
          }

          if (subInfo.getTermsOffsets().isEmpty()) {
            subInfoIterator.remove();
          }
        }
        fieldNameToFragInfos.get(field.name()).add(weightedFragInfo);
      }
    }

    List<WeightedFragInfo> result = new ArrayList<WeightedFragInfo>();
    for (List<WeightedFragInfo> weightedFragInfos : fieldNameToFragInfos.values()) {
      result.addAll(weightedFragInfos);
    }
    CollectionUtil.timSort(result, new Comparator<WeightedFragInfo>() {

      @Override
      public int compare(XFieldFragList.WeightedFragInfo info1, XFieldFragList.WeightedFragInfo info2) {
        return info1.getStartOffset() - info2.getStartOffset();
      }

    });

    return result;
  }

  public void setMultiValuedSeparator( char separator ){
    multiValuedSeparator = separator;
  }
  
  public char getMultiValuedSeparator(){
    return multiValuedSeparator;
  }

  public boolean isDiscreteMultiValueHighlighting() {
    return discreteMultiValueHighlighting;
  }

  public void setDiscreteMultiValueHighlighting(boolean discreteMultiValueHighlighting) {
    this.discreteMultiValueHighlighting = discreteMultiValueHighlighting;
  }

  protected String getPreTag( int num ){
    return getPreTag( preTags, num );
  }
  
  protected String getPostTag( int num ){
    return getPostTag( postTags, num );
  }
  
  protected String getPreTag( String[] preTags, int num ){
    int n = num % preTags.length;
    return preTags[n];
  }
  
  protected String getPostTag( String[] postTags, int num ){
    int n = num % postTags.length;
    return postTags[n];
  }
}
