FROM python:3.6-alpine

COPY . .

RUN ["pip3", "install", "pipenv"]

RUN apk update
RUN apk add make automake gcc g++ subversion python3-dev

RUN pipenv install
#--system --deploy --ignore-pipfile

CMD pipenv run python app.py