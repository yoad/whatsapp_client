# /addons/whatsapp_client/Dockerfile
FROM node:18-slim

# Install Chromium and its dependencies (required by whatsapp-web.js / Puppeteer)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    chromium \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Tell Puppeteer to skip downloading its own Chrome and use the system one
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Set working directory
WORKDIR /usr/src/app

# Force git to use HTTPS instead of SSH
RUN git config --global url."https://github.com/".insteadOf ssh://git@github.com/

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy all source files
COPY . .

# Expose port for ingress status page
EXPOSE 3001

# Run the client
CMD [ "node", "client.js" ]
