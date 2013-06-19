/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;

/**
 */
public interface IndexNumericFieldData<FD extends AtomicNumericFieldData> extends IndexFieldData<FD> {

    static enum NumericType {
        BYTE {
            @Override
            public boolean isFloatingPoint() {
                return false;
            }
            
            @Override
            public int requiredBits() {
                return 8;
            }
        },
        SHORT {
            @Override
            public boolean isFloatingPoint() {
                return false;
            }
            @Override
            public int requiredBits() {
                return 16;
            }
        },
        INT {
            @Override
            public boolean isFloatingPoint() {
                return false;
            }
            
            @Override
            public int requiredBits() {
                return 32;
            }
        },
        LONG {
            @Override
            public boolean isFloatingPoint() {
                return false;
            }
            
            @Override
            public int requiredBits() {
                return 64;
            }
        },
        FLOAT {
            @Override
            public boolean isFloatingPoint() {
                return true;
            }
            
            @Override
            public int requiredBits() {
                return 32;
            }
        },
        DOUBLE {
            @Override
            public boolean isFloatingPoint() {
                return true;
            }
            
            @Override
            public int requiredBits() {
                return 64;
            }
        };

        public abstract boolean isFloatingPoint();
        public abstract int requiredBits();
        public final TermsEnum wrapTermsEnum(TermsEnum termsEnum) {
            if (requiredBits() > 32) {
                return OrdinalsBuilder.wrapNumeric64Bit(termsEnum);
            } else {
                return OrdinalsBuilder.wrapNumeric32Bit(termsEnum);
            }
        }
    }

    NumericType getNumericType();

    /**
     * Loads the atomic field data for the reader, possibly cached.
     */
    FD load(AtomicReaderContext context);

    /**
     * Loads directly the atomic field data for the reader, ignoring any caching involved.
     */
    FD loadDirect(AtomicReaderContext context) throws Exception;
}
