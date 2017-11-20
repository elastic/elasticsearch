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

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of {@link DisableGraphAttribute}. */
public class DisableGraphAttributeImpl extends AttributeImpl implements DisableGraphAttribute {
    public DisableGraphAttributeImpl() {}

    @Override
    public void clear() {}

    @Override
    public void reflectWith(AttributeReflector reflector) {
    }

    @Override
    public void copyTo(AttributeImpl target) {}
}
