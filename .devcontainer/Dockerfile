FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# The base image ships a Yarn apt source whose upstream signing key was
# rotated. A stale key makes `apt-get update` fail, which then breaks any dev
# container Feature that installs apt packages (here: docker-in-docker) and
# drops the whole Codespace into recovery mode. This lab uses no Yarn, so
# remove the source before Features run.
RUN find /etc/apt -name '*.list' -type f -exec sed -i '/dl\.yarnpkg\.com/d' {} + \
 && rm -f /etc/apt/sources.list.d/yarn.list

RUN python -m pip install --no-cache-dir uv==0.12.9
