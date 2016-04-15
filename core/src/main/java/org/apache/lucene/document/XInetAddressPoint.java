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
package org.apache.lucene.document;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.SuppressForbidden;

/**
 * Forked utility methods from Lucene's InetAddressPoint until LUCENE-7232 and
 * LUCENE-7234 are released.
 */
// TODO: remove me when we upgrade to Lucene 6.1
@SuppressForbidden(reason="uses InetAddress.getHostAddress")
public final class XInetAddressPoint {

    private XInetAddressPoint() {}

    /** The minimum value that an ip address can hold. */
    public static final InetAddress MIN_VALUE;
    /** The maximum value that an ip address can hold. */
    public static final InetAddress MAX_VALUE;
    static {
      MIN_VALUE = InetAddressPoint.decode(new byte[InetAddressPoint.BYTES]);
      byte[] maxValueBytes = new byte[InetAddressPoint.BYTES];
      Arrays.fill(maxValueBytes, (byte) 0xFF);
      MAX_VALUE = InetAddressPoint.decode(maxValueBytes);
    }

    /**
     * Return the {@link InetAddress} that compares immediately greater than
     * {@code address}.
     * @throws ArithmeticException if the provided address is the
     *              {@link #MAX_VALUE maximum ip address}
     */
    public static InetAddress nextUp(InetAddress address) {
      if (address.equals(MAX_VALUE)) {
        throw new ArithmeticException("Overflow: there is no greater InetAddress than "
            + address.getHostAddress());
      }
      byte[] delta = new byte[InetAddressPoint.BYTES];
      delta[InetAddressPoint.BYTES-1] = 1;
      byte[] nextUpBytes = new byte[InetAddressPoint.BYTES];
      NumericUtils.add(InetAddressPoint.BYTES, 0, InetAddressPoint.encode(address), delta, nextUpBytes);
      return InetAddressPoint.decode(nextUpBytes);
    }

    /**
     * Return the {@link InetAddress} that compares immediately less than
     * {@code address}.
     * @throws ArithmeticException if the provided address is the
     *              {@link #MIN_VALUE minimum ip address}
     */
    public static InetAddress nextDown(InetAddress address) {
      if (address.equals(MIN_VALUE)) {
        throw new ArithmeticException("Underflow: there is no smaller InetAddress than "
            + address.getHostAddress());
      }
      byte[] delta = new byte[InetAddressPoint.BYTES];
      delta[InetAddressPoint.BYTES-1] = 1;
      byte[] nextDownBytes = new byte[InetAddressPoint.BYTES];
      NumericUtils.subtract(InetAddressPoint.BYTES, 0, InetAddressPoint.encode(address), delta, nextDownBytes);
      return InetAddressPoint.decode(nextDownBytes);
    }

    /** 
     * Create a prefix query for matching a CIDR network range.
     *
     * @param field field name. must not be {@code null}.
     * @param value any host address
     * @param prefixLength the network prefix length for this address. This is also known as the subnet mask in the context of IPv4
     * addresses.
     * @throws IllegalArgumentException if {@code field} is null, or prefixLength is invalid.
     * @return a query matching documents with addresses contained within this network
     */
    // TODO: remove me when we upgrade to Lucene 6.0.1
    public static Query newPrefixQuery(String field, InetAddress value, int prefixLength) {
      if (value == null) {
        throw new IllegalArgumentException("InetAddress must not be null");
      }
      if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
        throw new IllegalArgumentException("illegal prefixLength '" + prefixLength
                + "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
      }
      // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
      byte lower[] = value.getAddress();
      byte upper[] = value.getAddress();
      for (int i = prefixLength; i < 8 * lower.length; i++) {
        int m = 1 << (7 - (i & 7));
        lower[i >> 3] &= ~m;
        upper[i >> 3] |= m;
      }
      try {
        return InetAddressPoint.newRangeQuery(field, InetAddress.getByAddress(lower), InetAddress.getByAddress(upper));
      } catch (UnknownHostException e) {
        throw new AssertionError(e); // values are coming from InetAddress
      }
    }
}
