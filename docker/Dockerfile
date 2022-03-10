FROM golang:alpine

WORKDIR /opt/app

COPY ./trader-notifications/notifications /opt/app/notifications
COPY ./trader-notifications/config.json /opt/app/config.json

EXPOSE 5244

CMD ["/opt/app/notifications"]