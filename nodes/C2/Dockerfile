# Use the official .NET SDK image to build the app
FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /app

# Copy the csproj file and restore dependencies
COPY ./nodes/C2/*.csproj .
RUN dotnet restore

# Copy the rest of the application and build it
COPY ./nodes/C2/ .
RUN dotnet publish -c Release -o out

# Use the official .NET runtime image to run the app
FROM mcr.microsoft.com/dotnet/runtime:7.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .
COPY ./nodes/C2/appsettings.json .

# Run the application
ENTRYPOINT ["dotnet", "Consumer.dll"]