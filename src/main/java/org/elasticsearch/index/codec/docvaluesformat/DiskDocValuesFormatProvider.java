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

package org.elasticsearch.index.codec.docvaluesformat;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.diskdv.DiskDocValuesFormat;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 * A disk-based doc values format. This format takes no parameter.
 */
public class DiskDocValuesFormatProvider extends AbstractDocValuesFormatProvider {

    private final DocValuesFormat docValuesFormat;

    @Inject
    public DiskDocValuesFormatProvider(@Assisted String name, @Assisted Settings docValuesFormatSettings) {
        super(name);
        this.docValuesFormat = new DiskDocValuesFormat();
    }

    @Override
    public DocValuesFormat get() {
        return docValuesFormat;
    }
}
