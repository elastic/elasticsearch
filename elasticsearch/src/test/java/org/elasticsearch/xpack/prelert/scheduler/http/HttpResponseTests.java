/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public class HttpResponseTests extends ESTestCase {

    public void testGetResponseAsStream() throws IOException {
        InputStream stream = new ByteArrayInputStream("foo\nbar".getBytes(StandardCharsets.UTF_8));
        HttpResponse response = new HttpResponse(stream, 200);

        assertEquals("foo\nbar", response.getResponseAsString());
        assertEquals(200, response.getResponseCode());
    }

    public void testGetResponseAsStream_GivenStreamThrows() throws IOException {
        InputStream stream = mock(InputStream.class);
        HttpResponse response = new HttpResponse(stream, 200);

        try {
            response.getResponseAsString();
            fail();
        } catch (UncheckedIOException e) {
            verify(stream).close();
        }
    }
}
