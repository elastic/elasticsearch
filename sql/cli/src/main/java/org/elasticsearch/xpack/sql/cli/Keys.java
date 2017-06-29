/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.jline.keymap.KeyMap;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp.Capability;

import static org.jline.keymap.KeyMap.key;

class Keys {

    enum Key {
        UP,
        DOWN,
        LEFT,
        RIGHT,
        NONE
    }

    final KeyMap<Key> keys = new KeyMap<>();
    
    Keys(Terminal terminal) {
        keys.setNomatch(Key.NONE);
        keys.setAmbiguousTimeout(400);
        keys.bind(Key.UP, key(terminal, Capability.key_up));
        keys.bind(Key.DOWN, key(terminal, Capability.key_down));
        keys.bind(Key.RIGHT, key(terminal, Capability.key_right));
        keys.bind(Key.LEFT, key(terminal, Capability.key_left));
    }
}
