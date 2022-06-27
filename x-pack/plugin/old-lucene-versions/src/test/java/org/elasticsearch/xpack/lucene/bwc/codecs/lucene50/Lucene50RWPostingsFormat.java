/*
 * @notice
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
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree.Lucene40BlockTreeTermsWriter;

import java.io.IOException;

public class Lucene50RWPostingsFormat extends BWCLucene50PostingsFormat {

    public Lucene50RWPostingsFormat() {
        super("Lucene50RW");
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        PostingsWriterBase postingsWriter = new Lucene50PostingsWriter(state);
        boolean success = false;
        try {
            FieldsConsumer ret = new Lucene40BlockTreeTermsWriter(
                state,
                postingsWriter,
                Lucene40BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
                Lucene40BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE
            );
            success = true;
            return ret;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(postingsWriter);
            }
        }
    }

}
