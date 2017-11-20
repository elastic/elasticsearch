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
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.index.mapper.RangeFieldMapper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class InetAddressRandomBinaryDocValuesRangeQueryTests extends BaseRandomBinaryDocValuesRangeQueryTestCase {

    @Override
    protected String fieldName() {
        return "ip_range_dv_field";
    }

    @Override
    protected RangeFieldMapper.RangeType rangeType() {
        return RangeFieldMapper.RangeType.IP;
    }

    @Override
    protected Range nextRange(int dimensions) throws Exception {
        InetAddress min = nextInetaddress();
        byte[] bMin = InetAddressPoint.encode(min);
        InetAddress max = nextInetaddress();
        byte[] bMax = InetAddressPoint.encode(max);
        if (StringHelper.compare(bMin.length, bMin, 0, bMax, 0) > 0) {
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

            if (StringHelper.compare(e.length, min, 0, e, 0) < 0) {
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

            if (StringHelper.compare(e.length, max, 0, e, 0) > 0) {
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
            return StringHelper.compare(min.length, min, 0, other.max, 0) > 0 ||
                    StringHelper.compare(max.length, max, 0, other.min, 0) < 0;
        }

        @Override
        protected boolean isWithin(Range o) {
            IpRange other = (IpRange)o;
            return StringHelper.compare(min.length, min, 0, other.min, 0) >= 0 &&
                    StringHelper.compare(max.length, max, 0, other.max, 0) <= 0;
        }

        @Override
        protected boolean contains(Range o) {
            IpRange other = (IpRange)o;
            return StringHelper.compare(min.length, min, 0, other.min, 0) <= 0 &&
                    StringHelper.compare(max.length, max, 0, other.max, 0) >= 0;
        }

    }

}
