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
package org.apache.lucene5_shaded.analysis.tokenattributes;


import org.apache.lucene5_shaded.util.Attribute;

/**
 * A Token's lexical type. The Default value is "word". 
 */
public interface TypeAttribute extends Attribute {

  /** the default type */
  public static final String DEFAULT_TYPE = "word";

  /** 
   * Returns this Token's lexical type.  Defaults to "word". 
   * @see #setType(String)
   */
  public String type();

  /** 
   * Set the lexical type.
   * @see #type() 
   */
  public void setType(String type);
}
