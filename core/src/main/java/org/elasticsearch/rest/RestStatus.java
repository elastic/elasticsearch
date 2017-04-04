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

package org.elasticsearch.rest;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public enum RestStatus {
    /**
     * The client SHOULD continue with its request. This interim response is used to inform the client that the
     * initial part of the request has been received and has not yet been rejected by the server. The client
     * SHOULD continue by sending the remainder of the request or, if the request has already been completed,
     * ignore this response. The server MUST send a final response after the request has been completed.
     */
    CONTINUE(100),
    /**
     * The server understands and is willing to comply with the client's request, via the Upgrade message header field
     * (section 14.42), for a change in the application protocol being used on this connection. The server will
     * switch protocols to those defined by the response's Upgrade header field immediately after the empty line
     * which terminates the 101 response.
     */
    SWITCHING_PROTOCOLS(101),
    /**
     * The request has succeeded. The information returned with the response is dependent on the method
     * used in the request, for example:
     * <ul>
     * <li>GET: an entity corresponding to the requested resource is sent in the response;</li>
     * <li>HEAD: the entity-header fields corresponding to the requested resource are sent in the response without any message-body;</li>
     * <li>POST: an entity describing or containing the result of the action;</li>
     * <li>TRACE: an entity containing the request message as received by the end server.</li>
     * </ul>
     */
    OK(200),
    /**
     * The request has been fulfilled and resulted in a new resource being created. The newly created resource can
     * be referenced by the URI(s) returned in the entity of the response, with the most specific URI for the
     * resource given by a Location header field. The response SHOULD include an entity containing a list of resource
     * characteristics and location(s) from which the user or user agent can choose the one most appropriate. The
     * entity format is specified by the media type given in the Content-Type header field. The origin server MUST
     * create the resource before returning the 201 status code. If the action cannot be carried out immediately, the
     * server SHOULD respond with 202 (Accepted) response instead.
     * <p>
     * A 201 response MAY contain an ETag response header field indicating the current value of the entity tag
     * for the requested variant just created, see section 14.19.
     */
    CREATED(201),
    /**
     * The request has been accepted for processing, but the processing has not been completed.  The request might
     * or might not eventually be acted upon, as it might be disallowed when processing actually takes place. There
     * is no facility for re-sending a status code from an asynchronous operation such as this.
     * <p>
     * The 202 response is intentionally non-committal. Its purpose is to allow a server to accept a request for
     * some other process (perhaps a batch-oriented process that is only run once per day) without requiring that
     * the user agent's connection to the server persist until the process is completed. The entity returned with
     * this response SHOULD include an indication of the request's current status and either a pointer to a status
     * monitor or some estimate of when the user can expect the request to be fulfilled.
     */
    ACCEPTED(202),
    /**
     * The returned meta information in the entity-header is not the definitive set as available from the origin
     * server, but is gathered from a local or a third-party copy. The set presented MAY be a subset or super set
     * of the original version. For example, including local annotation information about the resource might
     * result in a super set of the meta information known by the origin server. Use of this response code
     * is not required and is only appropriate when the response would otherwise be 200 (OK).
     */
    NON_AUTHORITATIVE_INFORMATION(203),
    /**
     * The server has fulfilled the request but does not need to return an entity-body, and might want to return
     * updated meta information. The response MAY include new or updated meta information in the form of
     * entity-headers, which if present SHOULD be associated with the requested variant.
     * <p>
     * If the client is a user agent, it SHOULD NOT change its document view from that which caused the request
     * to be sent. This response is primarily intended to allow input for actions to take place without causing a
     * change to the user agent's active document view, although any new or updated meta information SHOULD be
     * applied to the document currently in the user agent's active view.
     * <p>
     * The 204 response MUST NOT include a message-body, and thus is always terminated by the first empty
     * line after the header fields.
     */
    NO_CONTENT(204),
    /**
     * The server has fulfilled the request and the user agent SHOULD reset the document view which caused the
     * request to be sent. This response is primarily intended to allow input for actions to take place via user
     * input, followed by a clearing of the form in which the input is given so that the user can easily initiate
     * another input action. The response MUST NOT include an entity.
     */
    RESET_CONTENT(205),
    /**
     * The server has fulfilled the partial GET request for the resource. The request MUST have included a Range
     * header field (section 14.35) indicating the desired range, and MAY have included an If-Range header
     * field (section 14.27) to make the request conditional.
     * <p>
     * The response MUST include the following header fields:
     * <ul>
     * <li>Either a Content-Range header field (section 14.16) indicating the range included with this response,
     * or a multipart/byteranges Content-Type including Content-Range fields for each part. If a Content-Length
     * header field is present in the response, its value MUST match the actual number of OCTETs transmitted in
     * the message-body.</li>
     * <li>Date</li>
     * <li>ETag and/or Content-Location, if the header would have been sent in a 200 response to the same request</li>
     * <li>Expires, Cache-Control, and/or Vary, if the field-value might differ from that sent in any previous
     * response for the same variant</li>
     * </ul>
     * <p>
     * If the 206 response is the result of an If-Range request that used a strong cache validator
     * (see section 13.3.3), the response SHOULD NOT include other entity-headers. If the response is the result
     * of an If-Range request that used a weak validator, the response MUST NOT include other entity-headers;
     * this prevents inconsistencies between cached entity-bodies and updated headers. Otherwise, the response MUST
     * include all of the entity-headers that would have been returned with a 200 (OK) response to the same request.
     * <p>
     * A cache MUST NOT combine a 206 response with other previously cached content if the ETag or Last-Modified
     * headers do not match exactly, see 13.5.4.
     * <p>
     * A cache that does not support the Range and Content-Range headers MUST NOT cache 206 (Partial) responses.
     */
    PARTIAL_CONTENT(206),
    /**
     * The 207 (Multi-Status) status code provides status for multiple independent operations (see Section 13 for
     * more information).
     * <p>
     * A Multi-Status response conveys information about multiple resources in situations where multiple status
     * codes might be appropriate. The default Multi-Status response body is a text/xml or application/xml HTTP
     * entity with a 'multistatus' root element. Further elements contain 200, 300, 400, and 500 series status codes
     * generated during the method invocation. 100 series status codes SHOULD NOT be recorded in a 'response'
     * XML element.
     * <p>
     * Although '207' is used as the overall response status code, the recipient needs to consult the contents
     * of the multistatus response body for further information about the success or failure of the method execution.
     * The response MAY be used in success, partial success and also in failure situations.
     * <p>
     * The 'multistatus' root element holds zero or more 'response' elements in any order, each with
     * information about an individual resource. Each 'response' element MUST have an 'href' element
     * to identify the resource.
     */
    MULTI_STATUS(207),
    /**
     * The requested resource corresponds to any one of a set of representations, each with its own specific
     * location, and agent-driven negotiation information (section 12) is being provided so that the user (or user
     * agent) can select a preferred representation and redirect its request to that location.
     * <p>
     * Unless it was a HEAD request, the response SHOULD include an entity containing a list of resource
     * characteristics and location(s) from which the user or user agent can choose the one most appropriate.
     * The entity format is specified by the media type given in the Content-Type header field. Depending upon the
     * format and the capabilities of the user agent, selection of the most appropriate choice MAY be performed
     * automatically. However, this specification does not define any standard for such automatic selection.
     * <p>
     * If the server has a preferred choice of representation, it SHOULD include the specific URI for that
     * representation in the Location field; user agents MAY use the Location field value for automatic redirection.
     * This response is cacheable unless indicated otherwise.
     */
    MULTIPLE_CHOICES(300),
    /**
     * The requested resource has been assigned a new permanent URI and any future references to this resource
     * SHOULD use one of the returned URIs.  Clients with link editing capabilities ought to automatically re-link
     * references to the Request-URI to one or more of the new references returned by the server, where possible.
     * This response is cacheable unless indicated otherwise.
     * <p>
     * The new permanent URI SHOULD be given by the Location field in the response. Unless the request method
     * was HEAD, the entity of the response SHOULD contain a short hypertext note with a hyperlink to the new URI(s).
     * <p>
     * If the 301 status code is received in response to a request other than GET or HEAD, the user agent
     * MUST NOT automatically redirect the request unless it can be confirmed by the user, since this might change
     * the conditions under which the request was issued.
     */
    MOVED_PERMANENTLY(301),
    /**
     * The requested resource resides temporarily under a different URI. Since the redirection might be altered on
     * occasion, the client SHOULD continue to use the Request-URI for future requests.  This response is only
     * cacheable if indicated by a Cache-Control or Expires header field.
     * <p>
     * The temporary URI SHOULD be given by the Location field in the response. Unless the request method was
     * HEAD, the entity of the response SHOULD contain a short hypertext note with a hyperlink to the new URI(s).
     * <p>
     * If the 302 status code is received in response to a request other than GET or HEAD, the user agent
     * MUST NOT automatically redirect the request unless it can be confirmed by the user, since this might change
     * the conditions under which the request was issued.
     */
    FOUND(302),
    /**
     * The response to the request can be found under a different URI and SHOULD be retrieved using a GET method on
     * that resource. This method exists primarily to allow the output of a POST-activated script to redirect the
     * user agent to a selected resource. The new URI is not a substitute reference for the originally requested
     * resource. The 303 response MUST NOT be cached, but the response to the second (redirected) request might be
     * cacheable.
     * <p>
     * The different URI SHOULD be given by the Location field in the response. Unless the request method was
     * HEAD, the entity of the response SHOULD contain a short hypertext note with a hyperlink to the new URI(s).
     */
    SEE_OTHER(303),
    /**
     * If the client has performed a conditional GET request and access is allowed, but the document has not been
     * modified, the server SHOULD respond with this status code. The 304 response MUST NOT contain a message-body,
     * and thus is always terminated by the first empty line after the header fields.
     * <p>
     * The response MUST include the following header fields:
     * <ul>
     * <li>Date, unless its omission is required by section 14.18.1
     * If a clockless origin server obeys these rules, and proxies and clients add their own Date to any
     * response received without one (as already specified by [RFC 2068], section 14.19), caches will operate
     * correctly.
     * </li>
     * <li>ETag and/or Content-Location, if the header would have been sent in a 200 response to the same request</li>
     * <li>Expires, Cache-Control, and/or Vary, if the field-value might differ from that sent in any previous
     * response for the same variant</li>
     * </ul>
     * <p>
     * If the conditional GET used a strong cache validator (see section 13.3.3), the response SHOULD NOT include
     * other entity-headers. Otherwise (i.e., the conditional GET used a weak validator), the response MUST NOT
     * include other entity-headers; this prevents inconsistencies between cached entity-bodies and updated headers.
     * <p>
     * If a 304 response indicates an entity not currently cached, then the cache MUST disregard the response
     * and repeat the request without the conditional.
     * <p>
     * If a cache uses a received 304 response to update a cache entry, the cache MUST update the entry to
     * reflect any new field values given in the response.
     */
    NOT_MODIFIED(304),
    /**
     * The requested resource MUST be accessed through the proxy given by the Location field. The Location field
     * gives the URI of the proxy. The recipient is expected to repeat this single request via the proxy.
     * 305 responses MUST only be generated by origin servers.
     */
    USE_PROXY(305),
    /**
     * The requested resource resides temporarily under a different URI. Since the redirection MAY be altered on
     * occasion, the client SHOULD continue to use the Request-URI for future requests.  This response is only
     * cacheable if indicated by a Cache-Control or Expires header field.
     * <p>
     * The temporary URI SHOULD be given by the Location field in the response. Unless the request method was
     * HEAD, the entity of the response SHOULD contain a short hypertext note with a hyperlink to the new URI(s) ,
     * since many pre-HTTP/1.1 user agents do not understand the 307 status. Therefore, the note SHOULD contain
     * the information necessary for a user to repeat the original request on the new URI.
     * <p>
     * If the 307 status code is received in response to a request other than GET or HEAD, the user agent MUST NOT
     * automatically redirect the request unless it can be confirmed by the user, since this might change the
     * conditions under which the request was issued.
     */
    TEMPORARY_REDIRECT(307),
    /**
     * The request could not be understood by the server due to malformed syntax. The client SHOULD NOT repeat the
     * request without modifications.
     */
    BAD_REQUEST(400),
    /**
     * The request requires user authentication. The response MUST include a WWW-Authenticate header field
     * (section 14.47) containing a challenge applicable to the requested resource. The client MAY repeat the request
     * with a suitable Authorization header field (section 14.8). If the request already included Authorization
     * credentials, then the 401 response indicates that authorization has been refused for those credentials.
     * If the 401 response contains the same challenge as the prior response, and the user agent has already attempted
     * authentication at least once, then the user SHOULD be presented the entity that was given in the response,
     * since that entity might include relevant diagnostic information. HTTP access authentication is explained in
     * "HTTP Authentication: Basic and Digest Access Authentication" [43].
     */
    UNAUTHORIZED(401),
    /**
     * This code is reserved for future use.
     */
    PAYMENT_REQUIRED(402),
    /**
     * The server understood the request, but is refusing to fulfill it. Authorization will not help and the request
     * SHOULD NOT be repeated. If the request method was not HEAD and the server wishes to make public why the
     * request has not been fulfilled, it SHOULD describe the reason for the refusal in the entity.  If the server
     * does not wish to make this information available to the client, the status code 404 (Not Found) can be used
     * instead.
     */
    FORBIDDEN(403),
    /**
     * The server has not found anything matching the Request-URI. No indication is given of whether the condition
     * is temporary or permanent. The 410 (Gone) status code SHOULD be used if the server knows, through some
     * internally configurable mechanism, that an old resource is permanently unavailable and has no forwarding
     * address. This status code is commonly used when the server does not wish to reveal exactly why the request
     * has been refused, or when no other response is applicable.
     */
    NOT_FOUND(404),
    /**
     * The method specified in the Request-Line is not allowed for the resource identified by the Request-URI.
     * The response MUST include an Allow header containing a list of valid methods for the requested resource.
     */
    METHOD_NOT_ALLOWED(405),
    /**
     * The resource identified by the request is only capable of generating response entities which have content
     * characteristics not acceptable according to the accept headers sent in the request.
     * <p>
     * Unless it was a HEAD request, the response SHOULD include an entity containing a list of available entity
     * characteristics and location(s) from which the user or user agent can choose the one most appropriate.
     * The entity format is specified by the media type given in the Content-Type header field. Depending upon the
     * format and the capabilities of the user agent, selection of the most appropriate choice MAY be performed
     * automatically. However, this specification does not define any standard for such automatic selection.
     * <p>
     * Note: HTTP/1.1 servers are allowed to return responses which are not acceptable according to the accept
     * headers sent in the request. In some cases, this may even be preferable to sending a 406 response. User
     * agents are encouraged to inspect the headers of an incoming response to determine if it is acceptable.
     * <p>
     * If the response could be unacceptable, a user agent SHOULD temporarily stop receipt of more data and query
     * the user for a decision on further actions.
     */
    NOT_ACCEPTABLE(406),
    /**
     * This code is similar to 401 (Unauthorized), but indicates that the client must first authenticate itself with
     * the proxy. The proxy MUST return a Proxy-Authenticate header field (section 14.33) containing a challenge
     * applicable to the proxy for the requested resource. The client MAY repeat the request with a suitable
     * Proxy-Authorization header field (section 14.34). HTTP access authentication is explained in
     * "HTTP Authentication: Basic and Digest Access Authentication" [43].
     */
    PROXY_AUTHENTICATION(407),
    /**
     * The client did not produce a request within the time that the server was prepared to wait. The client MAY
     * repeat the request without modifications at any later time.
     */
    REQUEST_TIMEOUT(408),
    /**
     * The request could not be completed due to a conflict with the current state of the resource. This code is
     * only allowed in situations where it is expected that the user might be able to resolve the conflict and
     * resubmit the request. The response body SHOULD include enough information for the user to recognize the
     * source of the conflict. Ideally, the response entity would include enough information for the user or user
     * agent to fix the problem; however, that might not be possible and is not required.
     * <p>
     * Conflicts are most likely to occur in response to a PUT request. For example, if versioning were being
     * used and the entity being PUT included changes to a resource which conflict with those made by an earlier
     * (third-party) request, the server might use the 409 response to indicate that it can't complete the request.
     * In this case, the response entity would likely contain a list of the differences between the two versions in
     * a format defined by the response Content-Type.
     */
    CONFLICT(409),
    /**
     * The requested resource is no longer available at the server and no forwarding address is known. This condition
     * is expected to be considered permanent. Clients with link editing capabilities SHOULD delete references to
     * the Request-URI after user approval. If the server does not know, or has no facility to determine, whether or
     * not the condition is permanent, the status code 404 (Not Found) SHOULD be used instead. This response is
     * cacheable unless indicated otherwise.
     * <p>
     * The 410 response is primarily intended to assist the task of web maintenance by notifying the recipient
     * that the resource is intentionally unavailable and that the server owners desire that remote links to that
     * resource be removed. Such an event is common for limited-time, promotional services and for resources belonging
     * to individuals no longer working at the server's site. It is not necessary to mark all permanently unavailable
     * resources as "gone" or to keep the mark for any length of time -- that is left to the discretion of the server
     * owner.
     */
    GONE(410),
    /**
     * The server refuses to accept the request without a defined Content-Length. The client MAY repeat the request
     * if it adds a valid Content-Length header field containing the length of the message-body in the request message.
     */
    LENGTH_REQUIRED(411),
    /**
     * The precondition given in one or more of the request-header fields evaluated to false when it was tested on
     * the server. This response code allows the client to place preconditions on the current resource metainformation
     * (header field data) and thus prevent the requested method from being applied to a resource other than the one
     * intended.
     */
    PRECONDITION_FAILED(412),
    /**
     * The server is refusing to process a request because the request entity is larger than the server is willing
     * or able to process. The server MAY close the connection to prevent the client from continuing the request.
     * <p>
     * If the condition is temporary, the server SHOULD include a Retry-After header field to indicate that it
     * is temporary and after what time the client MAY try again.
     */
    REQUEST_ENTITY_TOO_LARGE(413),
    /**
     * The server is refusing to service the request because the Request-URI is longer than the server is willing
     * to interpret. This rare condition is only likely to occur when a client has improperly converted a POST
     * request to a GET request with long query information, when the client has descended into a URI "black hole"
     * of redirection (e.g., a redirected URI prefix that points to a suffix of itself), or when the server is
     * under attack by a client attempting to exploit security holes present in some servers using fixed-length
     * buffers for reading or manipulating the Request-URI.
     */
    REQUEST_URI_TOO_LONG(414),
    /**
     * The server is refusing to service the request because the entity of the request is in a format not supported
     * by the requested resource for the requested method.
     */
    UNSUPPORTED_MEDIA_TYPE(415),
    /**
     * A server SHOULD return a response with this status code if a request included a Range request-header field
     * (section 14.35), and none of the range-specifier values in this field overlap the current extent of the
     * selected resource, and the request did not include an If-Range request-header field. (For byte-ranges, this
     * means that the first-byte-pos of all of the byte-range-spec values were greater than the current length of
     * the selected resource.)
     * <p>
     * When this status code is returned for a byte-range request, the response SHOULD include a Content-Range
     * entity-header field specifying the current length of the selected resource (see section 14.16). This
     * response MUST NOT use the multipart/byteranges content-type.
     */
    REQUESTED_RANGE_NOT_SATISFIED(416),
    /**
     * The expectation given in an Expect request-header field (see section 14.20) could not be met by this server,
     * or, if the server is a proxy, the server has unambiguous evidence that the request could not be met by the
     * next-hop server.
     */
    EXPECTATION_FAILED(417),
    /**
     * The 422 (Unprocessable Entity) status code means the server understands the content type of the request
     * entity (hence a 415(Unsupported Media Type) status code is inappropriate), and the syntax of the request
     * entity is correct (thus a 400 (Bad Request) status code is inappropriate) but was unable to process the
     * contained instructions. For example, this error condition may occur if an XML request body contains
     * well-formed (i.e., syntactically correct), but semantically erroneous, XML instructions.
     */
    UNPROCESSABLE_ENTITY(422),
    /**
     * The 423 (Locked) status code means the source or destination resource of a method is locked. This response
     * SHOULD contain an appropriate precondition or postcondition code, such as 'lock-token-submitted' or
     * 'no-conflicting-lock'.
     */
    LOCKED(423),
    /**
     * The 424 (Failed Dependency) status code means that the method could not be performed on the resource because
     * the requested action depended on another action and that action failed. For example, if a command in a
     * PROPPATCH method fails, then, at minimum, the rest of the commands will also fail with 424 (Failed Dependency).
     */
    FAILED_DEPENDENCY(424),
    /**
     * 429 Too Many Requests (RFC6585)
     */
    TOO_MANY_REQUESTS(429),
    /**
     * The server encountered an unexpected condition which prevented it from fulfilling the request.
     */
    INTERNAL_SERVER_ERROR(500),
    /**
     * The server does not support the functionality required to fulfill the request. This is the appropriate
     * response when the server does not recognize the request method and is not capable of supporting it for any
     * resource.
     */
    NOT_IMPLEMENTED(501),
    /**
     * The server, while acting as a gateway or proxy, received an invalid response from the upstream server it
     * accessed in attempting to fulfill the request.
     */
    BAD_GATEWAY(502),
    /**
     * The server is currently unable to handle the request due to a temporary overloading or maintenance of the
     * server. The implication is that this is a temporary condition which will be alleviated after some delay.
     * If known, the length of the delay MAY be indicated in a Retry-After header. If no Retry-After is given,
     * the client SHOULD handle the response as it would for a 500 response.
     */
    SERVICE_UNAVAILABLE(503),
    /**
     * The server, while acting as a gateway or proxy, did not receive a timely response from the upstream server
     * specified by the URI (e.g. HTTP, FTP, LDAP) or some other auxiliary server (e.g. DNS) it needed to access
     * in attempting to complete the request.
     */
    GATEWAY_TIMEOUT(504),
    /**
     * The server does not support, or refuses to support, the HTTP protocol version that was used in the request
     * message. The server is indicating that it is unable or unwilling to complete the request using the same major
     * version as the client, as described in section 3.1, other than with this error message. The response SHOULD
     * contain an entity describing why that version is not supported and what other protocols are supported by
     * that server.
     */
    HTTP_VERSION_NOT_SUPPORTED(505),
    /**
     * The 507 (Insufficient Storage) status code means the method could not be performed on the resource because
     * the server is unable to store the representation needed to successfully complete the request. This condition
     * is considered to be temporary. If the request that received this status code was the result of a user action,
     * the request MUST NOT be repeated until it is requested by a separate user action.
     */
    INSUFFICIENT_STORAGE(506);

    private static final Map<Integer, RestStatus> CODE_TO_STATUS;
    static {
        RestStatus[] values = values();
        Map<Integer, RestStatus> codeToStatus = new HashMap<>(values.length);
        for (RestStatus value : values) {
            codeToStatus.put(value.status, value);
        }
        CODE_TO_STATUS = unmodifiableMap(codeToStatus);
    }

    private int status;

    RestStatus(int status) {
        this.status = (short) status;
    }

    public int getStatus() {
        return status;
    }

    public static RestStatus readFrom(StreamInput in) throws IOException {
        return RestStatus.valueOf(in.readString());
    }

    public static void writeTo(StreamOutput out, RestStatus status) throws IOException {
        out.writeString(status.name());
    }

    public static RestStatus status(int successfulShards, int totalShards, ShardOperationFailedException... failures) {
        if (failures.length == 0) {
            if (successfulShards == 0 && totalShards > 0) {
                return RestStatus.SERVICE_UNAVAILABLE;
            }
            return RestStatus.OK;
        }
        RestStatus status = RestStatus.OK;
        if (successfulShards == 0 && totalShards > 0) {
            for (ShardOperationFailedException failure : failures) {
                RestStatus shardStatus = failure.status();
                if (shardStatus.getStatus() >= status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }
        return status;
    }

    /**
     * Turn a status code into a {@link RestStatus}, returning null if we don't know that status.
     */
    public static RestStatus fromCode(int code) {
        return CODE_TO_STATUS.get(code);
    }
}
