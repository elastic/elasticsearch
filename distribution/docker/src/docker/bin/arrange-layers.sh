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
# So that a failing find or sha256sum in the pipelines below fails the script, rather than only the last command of each
set -o pipefail

# Moves the paths read from the standard input, one per line, to the given layer. An empty layer is a mistake, such
# as a path that does not exist any more, so it fails.
move_to_layer() {
    layer=/layers/$1
    mkdir "$layer"
    moved=false
    while IFS= read -r path; do
        mkdir -p "$layer/$(dirname "$path")"
        mv "$path" "$layer/$path"
        moved=true
    done
    if ! $moved; then
        echo >&2 "Layer $1 is empty, the command that lists its paths printed none: $2"
        exit 1
    fi
}

# Prints the inodes of the jars below the given paths that have other hard links, without duplicates.
linked_jar_inodes() {
    find "$@" -type f -name '*.jar' -links +1 -exec stat -c %i {} + | sort -u
}

# A hard link cannot span two layers: if the links to a file were split between layers, each layer would store its
# own copy. So no hard-linked jar may have a link in another layer or in what is left in the home. Jars with
# identical content but different modes are not linked by `hardlink`, so they may be in different layers.
check_no_hard_link_is_split() {
    split_inodes=$({
        for layer in /layers/*; do
            # The status of a group is that of its last command, so a failure here would otherwise go unnoticed
            linked_jar_inodes "$layer" || exit 1
        done
        linked_jar_inodes lib modules plugins
    } | sort | uniq -d)
    if [ -n "$split_inodes" ]; then
        echo >&2 "Hard-linked jars would end up in different layers, listed by inode and path:"
        # Each line of $split_inodes is a pattern, so this keeps the jars that have one of those inodes
        find /layers lib modules plugins -type f -name '*.jar' -exec stat -c '%i %n' {} + | sort | grep -wF "$split_inodes" >&2
        exit 1
    fi
}

mkdir /layers

layer_number=0
for paths_command in "$@"; do
    sh -c "$paths_command" | move_to_layer "$layer_number" "$paths_command"
    layer_number=$((layer_number + 1))
done

check_no_hard_link_is_split
