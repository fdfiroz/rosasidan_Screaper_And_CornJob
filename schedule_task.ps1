# PowerShell script to create a scheduled task for the scraper
$Action = New-ScheduledTaskAction -Execute "python" -Argument "`$PSScriptRoot\scraper.py"
$Trigger = New-ScheduledTaskTrigger -Daily -At 10am
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

# Create the task
Register-ScheduledTask -TaskName "RosasidanScraper" -Action $Action -Trigger $Trigger -Settings $Settings -Description "Daily scraping of Rosasidan profiles at 10 AM"

Write-Host "Scheduled task 'RosasidanScraper' has been created successfully."
Write-Host "The scraper will run daily at 10:00 AM."