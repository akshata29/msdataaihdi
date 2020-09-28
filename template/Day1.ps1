#----------------------------------------------------------------#
#   Parameters                                                   #
#----------------------------------------------------------------#
Param (
    [Parameter(Mandatory=$true)]
    [string]$uniqueName = "default", 
    [Parameter(Mandatory=$false)]
    [string]$subscriptionId = "default",
    [Parameter(Mandatory=$false)]
    [string]$location = "eastus",
    [Parameter(Mandatory=$false)]
    [string]$resourceGroupName = $uniqueName,
	[Parameter(Mandatory=$true)]
    [string]$signInName = "default"
)

Write "Unique Name "  $uniqueName
Write "SignIn Name "  $signInName
if($uniqueName -eq "default")
{
    while ($TRUE) {
		try {
			$uniqueName = Read-Host -Prompt "Input UniqueName: "
			break  
		}
		catch {
			Write-Error "Please specify a unique name"
		}
	}
}

if($uniqueName.Length -gt 17)
{
    Write-Error "The unique name is too long. Please specify a name with less than 17 characters."
}

if($uniqueName -Match "-")
{
	Write-Error "The unique name should not contain special characters"
}

if($signInName -eq "default")
{
    while ($TRUE) {
		try {
			$signInName = Read-Host -Prompt "Input User Sign-in name: "
			break  
		}
		catch {
			Write-Error "Please specify a sign-in name"
		}
	}
}

if($location -eq "default")
{
	while ($TRUE) {
		try {
			$location = Read-Host -Prompt "Input Location(westus, eastus, centralus, southcentralus): "
			break  
		}
		catch {
				Write-Error "Please specify a resource group name."
		}
	}
}

Function Pause ($Message = "Press any key to continue...") {
   # Check if running in PowerShell ISE
   If ($psISE) {
      # "ReadKey" not supported in PowerShell ISE.
      # Show MessageBox UI
      New-Object -ComObject "WScript.Shell"
      Return
   }
 
   $Ignore =
      16,  # Shift (left or right)
      17,  # Ctrl (left or right)
      18,  # Alt (left or right)
      20,  # Caps lock
      91,  # Windows key (left)
      92,  # Windows key (right)
      93,  # Menu key
      144, # Num lock
      145, # Scroll lock
      166, # Back
      167, # Forward
      168, # Refresh
      169, # Stop
      170, # Search
      171, # Favorites
      172, # Start/Home
      173, # Mute
      174, # Volume Down
      175, # Volume Up
      176, # Next Track
      177, # Previous Track
      178, # Stop Media
      179, # Play
      180, # Mail
      181, # Select Media
      182, # Application 1
      183  # Application 2
 
   Write-Host -NoNewline $Message -ForegroundColor Red
   While ($Null -eq $KeyInfo.VirtualKeyCode  -Or $Ignore -Contains $KeyInfo.VirtualKeyCode) {
      $KeyInfo = $Host.UI.RawUI.ReadKey("NoEcho, IncludeKeyDown")
   }
}

$uniqueName = $uniqueName.ToLower();

# prefixes
$prefix = $uniqueName

if ( $resourceGroupName -eq 'default' ) {
	$resourceGroupName = $prefix
}

if ($ScriptRoot -eq "" -or $null -eq $ScriptRoot ) {
	$ScriptRoot = (Get-Location).path
}

#----------------------------------------------------------------#
#   Setup - Azure Subscription Login				 #
#----------------------------------------------------------------#
$ErrorActionPreference = "Stop"

#Install-Module -Name Az.Resources -Force
#Install-Module -Name Az -AllowClobber -Force

try
{
	Import-Module -Name Az.Resources -Force
	}
	catch
	{
	}


# Sign In
#Connect-AzAccount

Write-Host Logging in... -ForegroundColor Green
try {
	Get-AzContext
}
catch {
	Connect-AzAccount
}

if($subscriptionId -eq "default"){
	# Set Subscription Id
	while ($TRUE) {
		try {
			$subscriptionId = Read-Host -Prompt "Input subscription Id"
			break  
		}
		catch {
			Write-Host Invalid subscription Id. -ForegroundColor Green `n
		}
	}
}

$context = Get-AzSubscription -SubscriptionId $subscriptionId
Set-AzContext @context

$tenantId = $context.TenantId
$subscriptionName = $context.Name

Enable-AzContextAutosave -Scope CurrentUser
$index = 0
$numbers = "123456789"
foreach ($char in $subscriptionId.ToCharArray()) {
    if ($numbers.Contains($char)) {
        break;
    }
    $index++
}
$id = $subscriptionId.Substring($index, $index + 5)

Write-Host Unique Id $id... -ForegroundColor Green

# Create Resource Group 
Write-Host `nCreating Resource Group $resourceGroupName"..." -ForegroundColor Green `n
try {
		Get-AzResourceGroup `
			-Name $resourceGroupName `
			-Location $location `
	}
catch {
		New-AzResourceGroup `
			-Name $resourceGroupName `
			-Location $location `
			-Force
	}


$deAdfTemplateFilePath = "$ScriptRoot\Day1.json"
$deAdfParametersFilePath = "$ScriptRoot\Day1Param.json"
$deAdfParametersTemplate = Get-Content $deAdfParametersFilePath | ConvertFrom-Json
$deAdfParameters = $deAdfParametersTemplate.parameters
$deAdfParameters.uniqueName.value = $uniqueName
$deAdfParametersTemplate | ConvertTo-Json | Out-File $deAdfParametersFilePath

$templateName = $prefix + "tpl"

$deAdfParameters

Write-Host Deploying $templateName "..." -ForegroundColor Green
New-AzResourceGroupDeployment `
		-ResourceGroupName $resourceGroupName `
		-Name $templateName `
		-TemplateFile $deAdfTemplateFilePath `
		-TemplateParameterFile $deAdfParametersFilePath

# Assign Role assignments
# Grant Service principal Contributor access to ADLS ( or use Pass Through Authentication )
# Get ObjectId
$subScope = "/subscriptions/" + $subscriptionId + "/resourceGroups/" + $resourceGroupName

try {
		# Grant permission as Contributor at RG level
		New-AzRoleAssignment -SignInName $signInName `
			-RoleDefinitionName "Contributor" `
			-ResourceGroupName $resourceGroupName 
			#-Scope $subScope
			
	}
	catch {
		Write-Host Assigned Permission ... -ForegroundColor Green	
	}
	
$storageAccountName = $uniqueName + 'stor'
$adlsScope = $subScope + "/providers/Microsoft.Storage/storageAccounts/" + $storageAccountName
Write-Host Get Role Assignment - $adlsScope ... -ForegroundColor Green
$roleAssgn = Get-AzRoleAssignment -SignInName $signInName -Scope $adlsScope

New-AzRoleAssignment -SignInName $signInName `
-RoleDefinitionName "Storage Blob Data Contributor" `
-Scope  $adlsScope

New-AzRoleAssignment -SignInName $signInName `
-RoleDefinitionName "Storage Blob Data Owner" `
-Scope  $adlsScope


# AzCopy - Raw data
$context = (Get-AzStorageAccount -ResourceGroupName $resourceGroupName -AccountName $storageAccountName).context

$sasToken = New-AzStorageAccountSASToken -Context $context -Service Blob,File,Table,Queue -ResourceType Service,Container,Object -Permission racwdlup
$destUrl = "https://" + $storageAccountName + ".blob.core.windows.net/hdidatalake" + $sasToken

./azcopy cp "https://msdataainyctaxi.blob.core.windows.net/nyctaxi/nyctaxiraw/" $destUrl --recursive=true

$destUrlNb = "https://" + $storageAccountName + ".blob.core.windows.net/hdidatalake/HdiNotebooks" + $sasToken
./azcopy cp "https://msdataainyctaxi.blob.core.windows.net/nyctaxi/notebooks/*" $destUrlNb --recursive=true


