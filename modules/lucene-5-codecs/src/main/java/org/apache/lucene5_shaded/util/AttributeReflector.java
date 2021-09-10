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
package org.apache.lucene5_shaded.util;


/**
 * This interface is used to reflect contents of {@link AttributeSource} or {@link AttributeImpl}.
 */
public interface AttributeReflector {

  /**
   * This method gets called for every property in an {@link AttributeImpl}/{@link AttributeSource}
   * passing the class name of the {@link Attribute}, a key and the actual value.
   * E.g., an invocation of {@link org.apache.lucene5_shaded.analysis.tokenattributes.CharTermAttributeImpl#reflectWith}
   * would call this method once using {@code org.apache.lucene5_shaded.analysis.tokenattributes.CharTermAttribute.class}
   * as attribute class, {@code "term"} as key and the actual value as a String.
   */
  public void reflect(Class<? extends Attribute> attClass, String key, Object value);
  
}
