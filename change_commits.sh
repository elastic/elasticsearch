git filter-branch --env-filter '
  if [ "$GIT_COMMITTER_EMAIL" = "gal@pinecone.io" ]; then
    export GIT_COMMITTER_NAME="Gal Lalouche"
    export GIT_COMMITTER_EMAIL="gal.lalouche@elastic.co"
  fi
  if [ "$GIT_AUTHOR_EMAIL" = "gal@pinecone.io" ]; then
    export GIT_AUTHOR_NAME="Gal Lalouche"
    export GIT_AUTHOR_EMAIL="gal.lalouche@elastic.co"
  fi
' --tag-name-filter cat -- HEAD~1000..HEAD
