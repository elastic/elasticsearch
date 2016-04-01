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
package org.elasticsearch.index.mapper.ip;

import java.net.InetAddress;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

public class IpFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new IpFieldMapper.IpFieldType();
    }

    public void testValueFormat() throws Exception {
        MappedFieldType ft = createDefaultFieldType();
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));
    }

    public void testValueForSearch() throws Exception {
        MappedFieldType ft = createDefaultFieldType();
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForSearch(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForSearch(asBytes));
    }

    public void testTermQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("field");

        String ip = "2001:db8::2:1";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "192.168.1.7";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64), ft.termQuery(prefix, null));

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16), ft.termQuery(prefix, null));
    }
}
