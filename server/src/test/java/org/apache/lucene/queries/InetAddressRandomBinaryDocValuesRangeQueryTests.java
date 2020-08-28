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
package org.apache.lucene.queries;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.index.mapper.RangeType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class InetAddressRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "ip_range_dv_field";
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.IP;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        InetAddress min = nextInetaddress();
        byte[] bMin = InetAddressPoint.encode(min);
        InetAddress max = nextInetaddress();
        byte[] bMax = InetAddressPoint.encode(max);
        if (FutureArrays.compareUnsigned(bMin, 0, bMin.length, bMax, 0, bMin.length) > 0) {
            return new IpRange(max, min);
        }
        return new IpRange(min, max);
    }

    private InetAddress nextInetaddress() throws UnknownHostException {
        byte[] b = random().nextBoolean() ? new byte[4] : new byte[16];
        switch (random().nextInt(5)) {
            case 0:
                return InetAddress.getByAddress(b);
            case 1:
                Arrays.fill(b, (byte) 0xff);
                return InetAddress.getByAddress(b);
            case 2:
                Arrays.fill(b, (byte) 42);
                return InetAddress.getByAddress(b);
            default:
                random().nextBytes(b);
                return InetAddress.getByAddress(b);
        }
    }

    private static class IpRange extends AbstractRange<InetAddress> {
        InetAddress minAddress;
        InetAddress maxAddress;
        byte[] min;
        byte[] max;

        IpRange(InetAddress min, InetAddress max) {
            this.minAddress = min;
            this.maxAddress = max;
            this.min = InetAddressPoint.encode(min);
            this.max = InetAddressPoint.encode(max);
        }

        @Override
        public InetAddress getMin() {
            return minAddress;
        }

        @Override
        protected void setMin(int dim, Object val) {
            assert dim == 0;
            InetAddress v = (InetAddress)val;
            byte[] e = InetAddressPoint.encode(v);

            if (FutureArrays.compareUnsigned(min, 0, e.length, e, 0, e.length) < 0) {
                max = e;
                maxAddress = v;
            } else {
                min = e;
                minAddress = v;
            }
        }

        @Override
        public InetAddress getMax() {
            return maxAddress;
        }

        @Override
        protected void setMax(int dim, Object val) {
            assert dim == 0;
            InetAddress v = (InetAddress)val;
            byte[] e = InetAddressPoint.encode(v);

            if (FutureArrays.compareUnsigned(max, 0, e.length,  e, 0, e.length) > 0) {
                min = e;
                minAddress = v;
            } else {
                max = e;
                maxAddress = v;
            }
        }

        @Override
        protected boolean isDisjoint(Range o) {
            IpRange other = (IpRange) o;
            return FutureArrays.compareUnsigned(min, 0, min.length, other.max, 0, min.length) > 0 ||
                    FutureArrays.compareUnsigned(max, 0, max.length, other.min, 0, max.length) < 0;
        }

        @Override
        protected boolean isWithin(Range o) {
            IpRange other = (IpRange)o;
            return FutureArrays.compareUnsigned(min, 0, min.length, other.min, 0, min.length) >= 0 &&
                    FutureArrays.compareUnsigned(max, 0, max.length, other.max, 0, max.length) <= 0;
        }

        @Override
        protected boolean contains(Range o) {
            IpRange other = (IpRange)o;
            return FutureArrays.compareUnsigned(min, 0, min.length, other.min, 0, min.length) <= 0 &&
                    FutureArrays.compareUnsigned(max, 0, max.length, other.max, 0, max.length) >= 0;
        }

    }

}
