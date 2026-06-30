# /addons/whatsapp_client/Dockerfile
# v2.0.0 — Baileys engine (no Chromium/Puppeteer needed!)
FROM node:20-slim

# No Chromium dependencies needed with Baileys!
# Just install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/src/app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy all source files
COPY . .

# Expose port for ingress status page
EXPOSE 3001

# Run the client
CMD [ "node", "client.js" ]
