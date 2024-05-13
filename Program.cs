using System.Text;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

var redis = ConnectionMultiplexer.Connect(
    Environment.GetEnvironmentVariable("REDISHOSTNAME") 
    ?? throw new ArgumentNullException("REDISHOSTNAME not set in Environment")
);

var cache = redis.GetDatabase();
var db = new ShortUrlDbContext();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/shorten/url", (UrlShortnerRequest request) =>
{
    Trace($"POST Request received. Shorten URL = {request.Url}");
    string hash = request.Url.ToShortUrl();
    request.ShortUrl = $"{Environment.GetEnvironmentVariable("HOSTNAME") ?? throw new ArgumentNullException("HOSTNAME not set in Environment")}{hash}";
    
    Trace($"Update Cache with Key: {hash} Value: {request.Url}");
    cache.StringSet(hash, request.Url);

    Trace($"Write to Cache with Key: {hash} Value: {request.Url}");
    var shortUrlItem = db.ShortUrls.Where(item => item.Hash == hash);
    if(!shortUrlItem.Any())
    {
        Trace($"Write to Database with Hash: {hash} Target: {request.Url}");
        db.ShortUrls.Add(
            new ShortUrlItem
            {
                Hash = hash,
                Target = request.Url
            }
        );
        db.SaveChanges();
    }

    return request;
})
.WithName("ShortenUrl")
.WithOpenApi();

app.MapGet("/{hash}", (string hash) =>
{
    if(hash is null) return Results.Empty;

    Trace($"GET Request received for '{hash}'");
    Trace("Try to get from Cache...");
    string url = cache.StringGet(hash);
    if(url is null)
    {
        Trace("Not found in Cache. Try to read from Database");
        var shortUrlItem = db.ShortUrls.Where(item => item.Hash == hash);

        if(!shortUrlItem.Any()) 
        {
            Trace($"{hash} not found in Database.");
            return Results.Empty;
        }
        
        url = shortUrlItem.First().Target;
    }

    Trace($"Found {url} for Hash {hash}");
    return Results.Redirect(url, permanent: true);
})
.WithName("Redirect")
.WithOpenApi();

app.Run();


void Trace(string message) 
{
    Console.WriteLine($"{DateTime.Now.ToUniversalTime()} - {message}");
}

public class UrlShortnerRequest 
{
    public string Url { get; set;}
    public string ShortUrl { get; set;}
}

public static class StringExtension
{
    public static string ToShortUrl(this string value)
    {
        string originalUrl = value;
        var hash = System.IO.Hashing.Crc32.Hash(System.Text.Encoding.UTF8.GetBytes(originalUrl));
        
        StringBuilder stringBuilder = new StringBuilder();

        foreach (byte b in hash)
            stringBuilder.AppendFormat("{0:X2}", b);

        return stringBuilder.ToString(); 
    }
}

public class ShortUrlDbContext : DbContext
{
    public DbSet<ShortUrlItem> ShortUrls { get; set; }
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(Environment.GetEnvironmentVariable("POSTGRES_CONNECTION") ?? throw new ArgumentNullException("POSTGRES_CONNECTION not set in Environment"));
}

public class ShortUrlItem
{
    public int Id { get; set; }
    public string Hash { get; set; }
    public string Target { get; set; }
}