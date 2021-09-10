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
package org.apache.lucene5_shaded.index;


import java.io.IOException;

import org.apache.lucene5_shaded.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene5_shaded.util.InfoStream;

/** 
 * A very simple merged segment warmer that just ensures 
 * data structures are initialized.
 */
public class SimpleMergedSegmentWarmer extends IndexReaderWarmer {
  private final InfoStream infoStream;
  
  /**
   * Creates a new SimpleMergedSegmentWarmer
   * @param infoStream InfoStream to log statistics about warming.
   */
  public SimpleMergedSegmentWarmer(InfoStream infoStream) {
    this.infoStream = infoStream;
  }
  
  @Override
  public void warm(LeafReader reader) throws IOException {
    long startTime = System.currentTimeMillis();
    int indexedCount = 0;
    int docValuesCount = 0;
    int normsCount = 0;
    for (FieldInfo info : reader.getFieldInfos()) {
      if (info.getIndexOptions() != IndexOptions.NONE) {
        reader.terms(info.name); 
        indexedCount++;
        
        if (info.hasNorms()) {
          reader.getNormValues(info.name);
          normsCount++;
        }
      }
      
      if (info.getDocValuesType() != DocValuesType.NONE) {
        switch(info.getDocValuesType()) {
          case NUMERIC:
            reader.getNumericDocValues(info.name);
            break;
          case BINARY:
            reader.getBinaryDocValues(info.name);
            break;
          case SORTED:
            reader.getSortedDocValues(info.name);
            break;
          case SORTED_NUMERIC:
            reader.getSortedNumericDocValues(info.name);
            break;
          case SORTED_SET:
            reader.getSortedSetDocValues(info.name);
            break;
          default:
            assert false; // unknown dv type
        }
        docValuesCount++;
      }   
    }
    
    reader.document(0);
    reader.getTermVectors(0);
    
    if (infoStream.isEnabled("SMSW")) {
      infoStream.message("SMSW", 
             "Finished warming segment: " + reader + 
             ", indexed=" + indexedCount + 
             ", docValues=" + docValuesCount +
             ", norms=" + normsCount +
             ", time=" + (System.currentTimeMillis() - startTime));
    }
  }
}
