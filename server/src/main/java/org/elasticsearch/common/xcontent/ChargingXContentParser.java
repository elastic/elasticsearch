/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Charges a {@link ParsedContentReservation} for every token read through it, so that a request is charged for the heap its parsing
 * takes. A string value costs {@link #STRING_VALUE_BYTES} and any other token {@link #TOKEN_BYTES}, plus {@link #BYTES_PER_CHAR} per
 * character of a string value or field name. Characters are counted decoded, since that is what the parsed strings hold.
 *
 * <p>The costs are upper bounds of what parsing retains, measured with compressed object pointers, see
 * {@code ParsedContentChargeCalibrationTests}. Without them objects are larger, and the parent breaker's check of the real heap bounds
 * the difference.
 *
 * <p>The breaker is checked once per {@link #CHUNK_BYTES} the request charges, counted across all its parsers, so a typical request is not
 * checked while it is parsed. What is left is charged without a check when the root object or array ends, or at close if that comes first,
 * since many handlers never close their parsers. Parsers of one request that are open at the same time do not see each other's pending
 * charges, so each can charge up to a chunk without a check.
 *
 * <p>Once an object is parsed through {@link #namedObject} or {@link #parseSized}, the estimate for its own tokens is replaced with the
 * size it reports, see {@link SizedParsedObject}. The string of an object key is charged the first time this parser sees it, for up to
 * {@link #MAX_CHARGED_NAMES} names, since the parser hands every occurrence of a name the same string. Names are told apart by identity,
 * so a parser that makes a new string each time is charged each time.
 *
 * <p>Every token has to be read here. The bulk {@link #map()} and {@link #list()} methods and {@link #namedObject} would let the wrapped
 * parser read on its own, so they read through this parser. What {@link #skipChildren} skips is not charged, since skipping keeps nothing.
 * After a trip, any further charge trips again, since the counts no longer match the reservation.
 */
final class ChargingXContentParser extends FilterXContentParserWrapper implements ParsedObjectTracker {

    /**
     * An upper bound of the heap a token other than a string value takes once parsed. The most measured is about 43 bytes, in sibling
     * metric aggregations.
     */
    static final long TOKEN_BYTES = 56;

    /**
     * An upper bound of the heap a string value takes once parsed, besides its characters. The most measured is 84 bytes, for a
     * one-character field name in {@code sort}, which becomes a {@code FieldSortBuilder}.
     */
    static final long STRING_VALUE_BYTES = 104;

    /**
     * The heap a character of a string takes, at most. Query builders turn strings into {@code BytesRef}s, some sized for the worst case of
     * UTF-8, three bytes per character.
     */
    static final long BYTES_PER_CHAR = 3;

    static final long CHUNK_BYTES = ByteSizeUnit.MB.toBytes(1);

    /** Bounds the memory this parser takes to tell names apart. */
    static final int MAX_CHARGED_NAMES = 1024;

    private final ParsedContentReservation reservation;
    /** Whether the reservation is this parser's own, released when the parser is closed. */
    private final boolean ownsReservation;
    /** Charged but not yet passed to the reservation. A reported size below the estimate can make it negative. */
    private long pending;
    /** Nesting depth, to tell when the root ends. */
    private int depth;
    /** The trip that ended this parser, if the breaker tripped. */
    private CircuitBreakingException tripException;
    /** The {@link #pending} charge at which the request is checked, a chunk less what it charged without a check before. */
    private long checkAt;
    /** Everything this parser has charged, in the reservation and pending. */
    private long charged;
    /** What objects tracked inside the current tracked object were charged, which its size does not cover. */
    private long chargedInside;
    /** Field-name strings charged so far, at most {@link #MAX_CHARGED_NAMES}. Names beyond those are charged every time. */
    private final Set<String> chargedNames = Collections.newSetFromMap(new IdentityHashMap<>());

    ChargingXContentParser(XContentParser delegate, ParsedContentReservation reservation, boolean ownsReservation) {
        super(delegate);
        assert delegate.currentToken() == null : "a parser has to be tracked before it reads, so that the end of its root is known";
        this.reservation = reservation;
        this.ownsReservation = ownsReservation;
        this.checkAt = CHUNK_BYTES - reservation.uncheckedBytes();
    }

    @Override
    public Token nextToken() throws IOException {
        final Token token = delegate().nextToken();
        charge(token);
        return token;
    }

    @Override
    public String nextFieldName() throws IOException {
        final String name = delegate().nextFieldName();
        charge(delegate().currentToken());
        return name;
    }

    private void charge(Token token) throws IOException {
        if (token == null) {
            return;
        }
        final long bytes;
        if (token == Token.VALUE_STRING) {
            bytes = STRING_VALUE_BYTES + BYTES_PER_CHAR * delegate().textLength();
        } else if (token == Token.FIELD_NAME) {
            final String name = delegate().currentName();
            bytes = TOKEN_BYTES + BYTES_PER_CHAR * name.length();
            chargeName(name);
        } else {
            bytes = TOKEN_BYTES;
        }
        add(bytes);
        if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
            depth++;
        } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
            endContainer();
        }
    }

    /**
     * Does not charge what it skips, since skipping keeps none of it.
     */
    @Override
    public void skipChildren() throws IOException {
        final Token token = delegate().currentToken();
        delegate().skipChildren();
        if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
            endContainer();
        }
    }

    private void endContainer() {
        depth--;
        flushAtRoot();
    }

    /**
     * Passes what is pending to the reservation once the root has ended, since the handler may never close this parser.
     */
    private void flushAtRoot() {
        if (depth == 0) {
            flush();
            checkAt = CHUNK_BYTES - reservation.uncheckedBytes();
        }
    }

    /**
     * Charges a field-name string the first time this parser sees it. It counts as tracked inside the current object, so that the object's
     * size, which leaves names out, does not replace it.
     */
    private void chargeName(String name) {
        if (chargedNames.contains(name)) {
            return;
        }
        if (chargedNames.size() < MAX_CHARGED_NAMES) {
            chargedNames.add(name);
        }
        final long bytes = ParsedObjectSizes.string(name);
        add(bytes);
        chargedInside += bytes;
    }

    /**
     * Adds {@code bytes}, which can be negative, and passes the pending charge to the reservation once it reaches {@link #checkAt}, or once
     * a chunk can be released.
     */
    private void add(long bytes) {
        if (tripException != null) {
            throw new CircuitBreakingException(
                tripException.getMessage(),
                tripException.getBytesWanted(),
                tripException.getByteLimit(),
                tripException.getDurability()
            );
        }
        charged += bytes;
        pending += bytes;
        if (pending > 0 && pending >= checkAt) {
            final long toCharge = pending;
            pending = 0;
            try {
                reservation.reserve(toCharge);
            } catch (CircuitBreakingException e) {
                charged -= toCharge;
                tripException = e;
                throw e;
            }
            checkAt = CHUNK_BYTES - reservation.uncheckedBytes();
        } else if (pending <= -CHUNK_BYTES) {
            reservation.release(-pending);
            pending = 0;
        }
    }

    /**
     * Passes what is pending to the reservation without checking the limit, since the memory is in use already.
     */
    private void flush() {
        if (pending > 0) {
            reservation.reserveWithoutBreaking(pending);
        } else if (pending < 0) {
            reservation.release(-pending);
        }
        pending = 0;
    }

    @Override
    public <T> T parseSized(CheckedSupplier<T, IOException> parse, ToLongFunction<? super T> retainedBytes) throws IOException {
        final long chargedBefore = charged;
        final long chargedInsideBefore = chargedInside;
        chargedInside = 0;
        final T object;
        try {
            object = parse.get();
            final long size = retainedBytes.applyAsLong(object);
            // A negative size, such as UNKNOWN, keeps the estimate.
            if (size >= 0) {
                // Replaces the estimate for its own tokens. Objects tracked inside it keep their charges.
                add(size - (charged - chargedBefore - chargedInside));
                // If the object was the root, the root has ended already.
                flushAtRoot();
            }
        } finally {
            chargedInside = chargedInsideBefore + charged - chargedBefore;
        }
        return object;
    }

    @Override
    public Map<String, Object> map() throws IOException {
        requireSupportsMap();
        return AbstractXContentParser.readMap(this, HashMap::new);
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        requireSupportsMap();
        return AbstractXContentParser.readMap(this, LinkedHashMap::new);
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        requireSupportsMap();
        return AbstractXContentParser.readMap(this, HashMap::new, XContentParser::text);
    }

    @Override
    public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
        throws IOException {
        requireSupportsMap();
        return AbstractXContentParser.readMap(this, mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        requireSupportsList();
        return AbstractXContentParser.readList(this, HashMap::new);
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        requireSupportsList();
        return AbstractXContentParser.readList(this, LinkedHashMap::new);
    }

    private void requireSupportsMap() {
        if (supportsMap() == false) {
            throw new UnsupportedOperationException();
        }
    }

    private void requireSupportsList() {
        if (supportsList() == false) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return parseSized(() -> getXContentRegistry().parseNamedObject(categoryClass, name, this, context), SizedParsedObject::of);
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            try {
                super.close();
            } finally {
                if (ownsReservation) {
                    reservation.close();
                }
            }
        }
    }
}
