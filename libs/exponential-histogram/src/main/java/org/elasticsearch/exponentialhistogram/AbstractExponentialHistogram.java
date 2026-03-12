/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

/**
 * Basic implementation for {@link ExponentialHistogram} with common functionality.
 */
public abstract class AbstractExponentialHistogram implements ExponentialHistogram {

    @Override
    public long valueCount() {
        return zeroBucket().count() + negativeBuckets().valueCount() + positiveBuckets().valueCount();
    }

    @Override
    public int hashCode() {
        return ExponentialHistogram.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ExponentialHistogram) {
            return ExponentialHistogram.equals(this, (ExponentialHistogram) obj);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClassNameWithoutPackage()).append("{");
        sb.append("scale=").append(scale());
        sb.append(", sum=").append(sum());
        sb.append(", valueCount=").append(valueCount());
        sb.append(", min=").append(min());
        sb.append(", max=").append(max());
        ZeroBucket zb = zeroBucket();
        if (zb.zeroThreshold() != 0) {
            sb.append(", zeroThreshold=").append(zb.zeroThreshold());
        }
        if (zb.count() != 0) {
            sb.append(", zeroCount=").append(zb.count());
        }
        BucketIterator neg = negativeBuckets().iterator();
        if (neg.hasNext()) {
            sb.append(", negative=[");
            appendsBucketsAsString(sb, neg);
            sb.append("]");
        }
        BucketIterator pos = positiveBuckets().iterator();
        if (pos.hasNext()) {
            sb.append(", positive=[");
            appendsBucketsAsString(sb, pos);
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    private String getClassNameWithoutPackage() {
        String fqn = getClass().getName();
        int lastDot = fqn.lastIndexOf('.');
        return fqn.substring(lastDot + 1);
    }

    private static void appendsBucketsAsString(StringBuilder out, BucketIterator it) {
        boolean first = true;
        while (it.hasNext()) {
            if (first) {
                first = false;
            } else {
                out.append(", ");
            }
            out.append(it.peekIndex()).append(": ").append(it.peekCount());
            it.advance();
        }
    }
}
