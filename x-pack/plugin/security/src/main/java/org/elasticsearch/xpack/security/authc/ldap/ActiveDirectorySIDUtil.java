/* @notice
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/*
 * This code sourced from:
 * http://svn.apache.org/repos/asf/directory/studio/tags/2.0.0.v20170904-M13/plugins/valueeditors/src/main/java/org/apache/directory/studio/valueeditors/msad/InPlaceMsAdObjectSidValueEditor.java
 */

package org.elasticsearch.xpack.security.authc.ldap;

import org.apache.commons.codec.binary.Hex;

public class ActiveDirectorySIDUtil {
    public static final String TOKEN_GROUPS = "tokenGroups";
    public static String convertToString(byte[] bytes)
    {
        /*
         * The binary data structure, from http://msdn.microsoft.com/en-us/library/cc230371(PROT.10).aspx:
         *   byte[0] - Revision (1 byte): An 8-bit unsigned integer that specifies the revision level of
         *      the SID structure. This value MUST be set to 0x01.
         *   byte[1] - SubAuthorityCount (1 byte): An 8-bit unsigned integer that specifies the number of
         *      elements in the SubAuthority array. The maximum number of elements allowed is 15.
         *   byte[2-7] - IdentifierAuthority (6 bytes): A SID_IDENTIFIER_AUTHORITY structure that contains
         *      information, which indicates the authority under which the SID was created. It describes the
         *      entity that created the SID and manages the account.
         *      Six element arrays of 8-bit unsigned integers that specify the top-level authority
         *      big-endian!
         *   and then - SubAuthority (variable): A variable length array of unsigned 32-bit integers that
         *      uniquely identifies a principal relative to the IdentifierAuthority. Its length is determined
         *      by SubAuthorityCount. little-endian!
         */

        if ( ( bytes == null ) || ( bytes.length < 8 ) )
        {
            throw new IllegalArgumentException("Invalid SID");
        }

        char[] hex = Hex.encodeHex( bytes );
        StringBuffer sb = new StringBuffer();

        // start with 'S'
        sb.append( 'S' );

        // revision
        int revision = Integer.parseInt( new String( hex, 0, 2 ), 16 );
        sb.append( '-' );
        sb.append( revision );

        // get count
        int count = Integer.parseInt( new String( hex, 2, 2 ), 16 );

        // check length
        if ( bytes.length != ( 8 + count * 4 ) )
        {
            throw new IllegalArgumentException("Invalid SID");
        }

        // get authority, big-endian
        long authority = Long.parseLong( new String( hex, 4, 12 ), 16 );
        sb.append( '-' );
        sb.append( authority );

        // sub-authorities, little-endian
        for ( int i = 0; i < count; i++ )
        {
            StringBuffer rid = new StringBuffer();

            for ( int k = 3; k >= 0; k-- )
            {
                rid.append( hex[16 + ( i * 8 ) + ( k * 2 )] );
                rid.append( hex[16 + ( i * 8 ) + ( k * 2 ) + 1] );
            }

            long subAuthority = Long.parseLong( rid.toString(), 16 );
            sb.append( '-' );
            sb.append( subAuthority );
        }

        return sb.toString();
    }
}
