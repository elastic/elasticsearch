FROM node:24-slim

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY fetch_arrow.mjs .
USER node

ENTRYPOINT ["node", "/app/fetch_arrow.mjs"]
