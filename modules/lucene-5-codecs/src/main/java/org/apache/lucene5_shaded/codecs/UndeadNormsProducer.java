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
package org.apache.lucene5_shaded.codecs;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FieldInfos;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.util.Accountable;

/** 
 * Used only for backwards compatibility corner case, to provide
 * re-animated norms when all fields are undead.
 * 
 * @lucene.internal */
public class UndeadNormsProducer extends NormsProducer {

  /** Used to bring undead norms back to life. */
  public final static String LEGACY_UNDEAD_NORMS_KEY = UndeadNormsProducer.class.getSimpleName() + ".undeadnorms";

  /** Use this instance */
  public final static NormsProducer INSTANCE = new UndeadNormsProducer();

  private UndeadNormsProducer() {
  }

  /* Returns true if all indexed fields have undead norms. */
  public static boolean isUndeadArmy(FieldInfos fieldInfos) {

    boolean everythingIsUndead = true;
    for(FieldInfo fieldInfo : fieldInfos) {
      if (fieldInfo.hasNorms()) {
        String isUndead = fieldInfo.getAttribute(LEGACY_UNDEAD_NORMS_KEY);
        if (isUndead != null) {
          assert "true".equals(isUndead);
        } else {
          everythingIsUndead = false;
        }
      }
    }

    return everythingIsUndead;
  }

  /** Returns true if this field has undead norms. */
  public static boolean isUndead(FieldInfo fieldInfo) {
    String isUndead = fieldInfo.getAttribute(LEGACY_UNDEAD_NORMS_KEY);
    if (isUndead != null) {
      // Bring undead norms back to life; this is set in Lucene40FieldInfosFormat, to emulate pre-5.0 undead norms
      assert "true".equals(isUndead);
      return true;
    } else {
      return false;
    }
  }

  /** Call this to note that the field with these attributes has undead norms. */
  public static void setUndead(Map<String,String> attributes) {
    attributes.put(LEGACY_UNDEAD_NORMS_KEY, "true");
  }

  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    return DocValues.emptyNumeric();
  }
  
  @Override
  public void close() {
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public void checkIntegrity() throws IOException {
  }
  
  @Override
  public NormsProducer getMergeInstance() throws IOException {
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
