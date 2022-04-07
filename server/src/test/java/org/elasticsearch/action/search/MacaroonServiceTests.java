/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;

public class MacaroonServiceTests extends ESTestCase {

    public void testIsMacaroonValid() {
        boolean actual = new MacaroonService().isMacaroonValid(
            "MDAwZWxvY2F0aW9uIAowMDI5aWRlbnRpZmllciBjZmJiZ25kdGFlYmtwMGptcGt1aHUxd3JtCjAxNjBjaWQgcGl0X2lkID0gW2s0X3FBd0lQYlhrdGFXNWtaWGd0TURBd01EQXlGbGxETVdZMVdrMXRWRUZQUzI5T2RtMVpNVGRVT0ZFQUZrbFZibEJaWjI1WlUxcFhOVVl0WlVWRlZtWk5jR2NBQUFBQUFBQUFBQzBXYVdOWVQwNVJjMWhVTUVOSE5VODJjemh0Y21ST1VRQVBiWGt0YVc1a1pYZ3RNREF3TURBeEZtTnNUbVJXV0Y5alUyOXhibU42WW01WFN6ZFhhSGNBRmtsVmJsQlpaMjVaVTFwWE5VWXRaVVZGVm1aTmNHY0FBQUFBQUFBQUFDd1dhV05ZVDA1UmMxaFVNRU5ITlU4MmN6aHRjbVJPVVFBQ0ZsbERNV1kxV2sxdFZFRlBTMjlPZG0xWk1UZFVPRkVBQUJaamJFNWtWbGhmWTFOdmNXNWplbUp1VjBzM1YyaDNBQUE9XQowMDRkY2lkIHsKICAgIm1hdGNoIiA6IHsKICAgICAiYmFyIiA6IHsKICAgICAgICJxdWVyeSIgOiAiZm9vIgogICAgIH0KICAgfQp9CjAwMmZzaWduYXR1cmUgdSYO-HYTTcCn5DqpQwAmK2Z9jb4_663rhdHid_1jCqUK",
            "k4_qAwIPbXktaW5kZXgtMDAwMDAyFllDMWY1Wk1tVEFPS29Odm1ZMTdUOFEAFklVblBZZ25ZU1pXNUYtZUVFVmZNcGcAAAAAAAAAAC0WaWNYT05Rc1hUMENHNU82czhtcmROUQAPbXktaW5kZXgtMDAwMDAxFmNsTmRWWF9jU29xbmN6Ym5XSzdXaHcAFklVblBZZ25ZU1pXNUYtZUVFVmZNcGcAAAAAAAAAACwWaWNYT05Rc1hUMENHNU82czhtcmROUQACFllDMWY1Wk1tVEFPS29Odm1ZMTdUOFEAABZjbE5kVlhfY1NvcW5jemJuV0s3V2h3AAA=",
            QueryBuilders.matchQuery("bar", "foo")
        );
        assertTrue(actual);
    }
}
