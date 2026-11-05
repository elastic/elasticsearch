#!/bin/sh
#
# Prepares the Elasticsearch distribution in the current directory, the Elasticsearch home, to be copied to the
# final Docker image in layers of their own, so that the layers that do not change between builds can be reused.
#
# Each layer is moved, keeping its paths, to /layers/<n>. The final image copies the layers in order, from the
# least to the most frequently changing, and finally whatever is left in the home.
#
# Usage: arrange-layers.sh <command>...
#
# Each command prints, one per line, the paths that make up a layer, starting with the one that is copied first.

set -eu

# Many modules and plugins bundle identical copies of the same jar. Replace them with hard links, i.e. several
# paths to a single file, so that the file is stored only once in the image.
dedupe_jars() {
    previous_sum=
    find lib modules plugins -type f -name '*.jar' -exec sha256sum {} + | sort | while read -r sum path; do
        if [ "$sum" = "$previous_sum" ]; then
            ln -f "$first_path" "$path"
        else
            previous_sum=$sum
            first_path=$path
        fi
    done
}

# Moves the paths read from the standard input, one per line, to the given layer. An empty layer is a mistake, such
# as a path that does not exist any more, so it fails.
move_to_layer() {
    layer=/layers/$1
    mkdir "$layer"
    moved=false
    while read -r path; do
        mkdir -p "$layer/$(dirname "$path")"
        mv "$path" "$layer/$path"
        moved=true
    done
    if ! $moved; then
        echo >&2 "Layer $1 is empty, the command that lists its paths printed none: $2"
        exit 1
    fi
}

# Prints the checksums of the jars below the given paths, without duplicates.
jar_checksums() {
    find "$@" -type f -name '*.jar' -exec sha256sum {} + | cut -d' ' -f1 | sort -u
}

# A hard link cannot span two layers: if the copies of a jar were split between layers, each layer would store its
# own copy. So no jar may have a copy with identical content in another layer or in what is left in the home.
check_no_jar_is_split() {
    split_checksums=$({
        for layer in /layers/*; do
            jar_checksums "$layer"
        done
        jar_checksums lib modules plugins
    } | sort | uniq -d)
    if [ -n "$split_checksums" ]; then
        echo >&2 "Jars with identical content would end up in different layers"
        exit 1
    fi
}

dedupe_jars
mkdir /layers

layer_number=0
for paths_command in "$@"; do
    sh -c "$paths_command" | move_to_layer "$layer_number" "$paths_command"
    layer_number=$((layer_number + 1))
done

check_no_jar_is_split
